mod browsers;
mod config;
mod db;
mod state;
mod worker;

use std::sync::Arc;
use tauri::{AppHandle, Emitter, Manager, State};
use tauri::menu::{Menu, MenuItem};
use tauri::tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent};
use tauri_plugin_shell::ShellExt;
use tauri_plugin_shell::process::CommandEvent;
use state::{AppState, DownloadJob, DownloadStatus};
use config::Config;
use db::{HistoryEntry, HistoryStats};

type AppStateRef = Arc<AppState>;
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");

// ─── helpers ─────────────────────────────────────────────────────────────────

fn config_path(app: &AppHandle) -> Option<std::path::PathBuf> {
    app.path().app_data_dir().ok().map(|d| d.join("config.json"))
}

fn load_config(app: &AppHandle) -> Config {
    config_path(app)
        .and_then(|p| std::fs::read_to_string(p).ok())
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

fn save_config_to_disk(app: &AppHandle, cfg: &Config) {
    if let Some(path) = config_path(app) {
        if let Some(p) = path.parent() { let _ = std::fs::create_dir_all(p); }
        if let Ok(j) = serde_json::to_string_pretty(cfg) { let _ = std::fs::write(path, j); }
    }
}

fn init_db(app: &AppHandle) -> Option<db::Database> {
    let path = app.path().app_data_dir().ok()?.join("history.db");
    std::fs::create_dir_all(path.parent()?).ok()?;
    db::Database::new(&path).ok()
}

fn enqueue(id: String, url: String, fmt: String, quality: String, category_id: Option<String>, state: &AppStateRef, app: AppHandle) {
    let arc = state.clone();
    tauri::async_runtime::spawn(async move {
        worker::run(id, url, fmt, quality, category_id, arc, app).await;
    });
}

fn make_job(id: &str, url: &str, fmt: &str, quality: &str, category_id: Option<String>) -> DownloadJob {
    DownloadJob {
        id: id.to_string(), url: url.to_string(),
        title: None, thumbnail: None, duration: None, uploader: None,
        format_type: fmt.to_string(), quality: quality.to_string(), actual_quality: None,
        category_id,
        status: DownloadStatus::Fetching,
        progress: 0.0, speed: None, eta: None, size: None, output_path: None,
        codec: None, fps: None, filesize_approx: None,
    }
}

fn is_newer_version(latest: &str, current: &str) -> bool {
    let parse = |v: &str| -> [u32; 3] {
        let mut p = v.trim_start_matches('v').splitn(3, '.');
        [p.next().and_then(|x| x.parse().ok()).unwrap_or(0),
         p.next().and_then(|x| x.parse().ok()).unwrap_or(0),
         p.next().and_then(|x| x.parse().ok()).unwrap_or(0)]
    };
    parse(latest) > parse(current)
}

async fn do_update_check() -> Option<String> {
    let client = reqwest::Client::builder()
        .user_agent(concat!("catalyst/", env!("CARGO_PKG_VERSION")))
        .build().ok()?;
    let resp: serde_json::Value = client
        .get("https://api.github.com/repos/ZohanZaheer/Catalyst/releases/latest")
        .send().await.ok()?.json().await.ok()?;
    if resp["message"].as_str().is_some() { return None; } // 404 / no releases
    let tag = resp["tag_name"].as_str()?;
    if is_newer_version(tag, APP_VERSION) { Some(tag.to_string()) } else { None }
}

// ─── queue commands ──────────────────────────────────────────────────────────

#[tauri::command]
async fn add_download(
    url: String, format_type: Option<String>, quality: Option<String>,
    category_id: Option<String>,
    app: AppHandle, state: State<'_, AppStateRef>,
) -> Result<String, String> {
    let (fmt, qual) = {
        let cfg = state.config.lock().unwrap();
        (format_type.unwrap_or_else(|| cfg.default_format_type.clone()),
         quality.unwrap_or_else(||    cfg.default_quality.clone()))
    };
    let id = uuid::Uuid::new_v4().to_string();
    let job = make_job(&id, &url, &fmt, &qual, category_id.clone());
    state.jobs.lock().unwrap().insert(0, job.clone());
    let _ = app.emit("download-update", &job);
    enqueue(id.clone(), url, fmt, qual, category_id, state.inner(), app);
    Ok(id)
}

#[tauri::command]
async fn add_downloads_bulk(
    urls: Vec<String>, format_type: Option<String>, quality: Option<String>,
    category_id: Option<String>,
    app: AppHandle, state: State<'_, AppStateRef>,
) -> Result<usize, String> {
    let (fmt, qual) = {
        let cfg = state.config.lock().unwrap();
        (format_type.unwrap_or_else(|| cfg.default_format_type.clone()),
         quality.unwrap_or_else(||    cfg.default_quality.clone()))
    };
    let n = urls.len();
    for url in urls {
        let id = uuid::Uuid::new_v4().to_string();
        let job = make_job(&id, &url, &fmt, &qual, category_id.clone());
        state.jobs.lock().unwrap().insert(0, job.clone());
        let _ = app.emit("download-update", &job);
        enqueue(id, url, fmt.clone(), qual.clone(), category_id.clone(), state.inner(), app.clone());
    }
    Ok(n)
}

#[tauri::command]
fn get_queue(state: State<'_, AppStateRef>) -> Vec<DownloadJob> {
    state.jobs.lock().unwrap().clone()
}

/// Kill any tracked child process for `id` and mark the job Cancelled. Shared
/// by cancel_download and remove_job(s) — removing a job from the list must
/// never leave its yt-dlp process running unattended in the background.
fn cancel_internal(id: &str, state: &AppStateRef) {
    if let Some(child) = state.children.lock().unwrap().remove(id) { let _ = child.kill(); }
    state.update_job(id, |job| job.status = DownloadStatus::Cancelled);
}

#[tauri::command]
fn cancel_download(id: String, state: State<'_, AppStateRef>, app: AppHandle) -> Result<(), String> {
    cancel_internal(&id, state.inner());
    if let Some(job) = state.get_job(&id) { let _ = app.emit("download-update", job); }
    Ok(())
}

#[tauri::command]
async fn retry_download(id: String, state: State<'_, AppStateRef>, app: AppHandle) -> Result<(), String> {
    let (url, fmt, qual, cat) = {
        let jobs = state.jobs.lock().unwrap();
        let job = jobs.iter().find(|j| j.id == id).ok_or("Job not found")?;
        (job.url.clone(), job.format_type.clone(), job.quality.clone(), job.category_id.clone())
    };
    let new_job = make_job(&id, &url, &fmt, &qual, cat.clone());
    state.update_job(&id, |job| *job = new_job.clone());
    let _ = app.emit("download-update", &new_job);
    enqueue(id, url, fmt, qual, cat, state.inner(), app);
    Ok(())
}

#[tauri::command]
fn remove_job(id: String, state: State<'_, AppStateRef>) {
    // Removing an in-progress job from the list previously left its yt-dlp
    // process running orphaned in the background — kill it first.
    cancel_internal(&id, state.inner());
    state.jobs.lock().unwrap().retain(|j| j.id != id);
}

#[tauri::command]
fn remove_jobs(ids: Vec<String>, state: State<'_, AppStateRef>) {
    for id in &ids { cancel_internal(id, state.inner()); }
    let set: std::collections::HashSet<_> = ids.into_iter().collect();
    state.jobs.lock().unwrap().retain(|j| !set.contains(&j.id));
}

#[tauri::command]
fn clear_completed(state: State<'_, AppStateRef>) {
    state.jobs.lock().unwrap().retain(|j| {
        !matches!(j.status, DownloadStatus::Finished | DownloadStatus::Failed { .. } | DownloadStatus::Cancelled)
    });
}

#[tauri::command]
fn reorder_queue(ids: Vec<String>, state: State<'_, AppStateRef>) {
    let mut jobs = state.jobs.lock().unwrap();
    let mut reordered: Vec<DownloadJob> = ids.iter()
        .filter_map(|id| jobs.iter().find(|j| &j.id == id).cloned())
        .collect();
    for job in jobs.iter() {
        if !ids.contains(&job.id) { reordered.push(job.clone()); }
    }
    *jobs = reordered;
}

// ─── file / url commands ─────────────────────────────────────────────────────

#[tauri::command]
fn open_folder(path: String) -> Result<(), String> {
    let p = std::path::Path::new(&path);
    let dir = if p.is_dir() { p } else { p.parent().unwrap_or(p) };
    open::that(dir).map_err(|e| e.to_string())
}

#[tauri::command]
fn open_url(url: String) -> Result<(), String> {
    open::that(url).map_err(|e| e.to_string())
}

#[tauri::command]
fn delete_file(path: String) -> Result<(), String> {
    std::fs::remove_file(&path).map_err(|e| e.to_string())
}

#[tauri::command]
fn read_text_file(path: String) -> Result<String, String> {
    std::fs::read_to_string(&path).map_err(|e| e.to_string())
}

// ─── queue pause ─────────────────────────────────────────────────────────────

#[tauri::command]
fn set_queue_paused(paused: bool, state: State<'_, AppStateRef>) {
    *state.queue_paused.lock().unwrap() = paused;
    // A manual toggle (either direction) supersedes any auto-pause reason.
    *state.auto_pause_reason.lock().unwrap() = None;
}

#[tauri::command]
fn get_queue_paused(state: State<'_, AppStateRef>) -> bool {
    *state.queue_paused.lock().unwrap()
}

/// Why the queue is currently auto-paused, if it is — lets a fresh page
/// load (or a frontend that missed the "queue-auto-paused" event) show the
/// reason instead of just an unexplained paused toggle.
#[tauri::command]
fn get_auto_pause_reason(state: State<'_, AppStateRef>) -> Option<String> {
    state.auto_pause_reason.lock().unwrap().clone()
}

// ─── config commands ─────────────────────────────────────────────────────────

#[tauri::command]
fn get_config(state: State<'_, AppStateRef>) -> Config {
    state.config.lock().unwrap().clone()
}

#[tauri::command]
fn save_config(new_config: Config, state: State<'_, AppStateRef>, app: AppHandle) -> Result<(), String> {
    save_config_to_disk(&app, &new_config);
    // Settings has its own local Config state and only writes here — nothing
    // else in the app previously found out a save happened, so e.g. editing
    // output categories didn't update the Queue tab's category dropdown
    // until a full app reload. Broadcast the new config so any screen that
    // cares (currently just App.tsx) can stay in sync live.
    let _ = app.emit("config-updated", &new_config);
    *state.config.lock().unwrap() = new_config;
    Ok(())
}

// ─── history commands ────────────────────────────────────────────────────────

#[tauri::command]
fn get_history(limit: Option<usize>, state: State<'_, AppStateRef>) -> Vec<HistoryEntry> {
    state.db.as_ref().and_then(|db| db.get_all(limit.unwrap_or(1000)).ok()).unwrap_or_default()
}

#[tauri::command]
fn delete_history_entry(id: String, state: State<'_, AppStateRef>) -> Result<(), String> {
    state.db.as_ref().map(|db| db.delete(&id).map_err(|e| e.to_string())).unwrap_or(Ok(()))
}

#[tauri::command]
fn clear_history(state: State<'_, AppStateRef>) -> Result<(), String> {
    state.db.as_ref().map(|db| db.clear().map_err(|e| e.to_string())).unwrap_or(Ok(()))
}

#[tauri::command]
fn get_history_stats(state: State<'_, AppStateRef>) -> HistoryStats {
    state.db.as_ref().and_then(|db| db.get_stats().ok()).unwrap_or_default()
}

// ─── history pause ────────────────────────────────────────────────────────────

#[tauri::command]
fn set_history_pause(until: Option<i64>, state: State<'_, AppStateRef>) {
    *state.history_paused_until.lock().unwrap() = until;
}

#[tauri::command]
fn get_history_pause(state: State<'_, AppStateRef>) -> Option<i64> {
    *state.history_paused_until.lock().unwrap()
}

// ─── browser detection ───────────────────────────────────────────────────────

#[tauri::command]
fn detect_browsers() -> Vec<browsers::DetectedBrowser> { browsers::detect() }

// ─── yt-dlp management ───────────────────────────────────────────────────────

#[tauri::command]
async fn get_ytdlp_version(app: AppHandle) -> Result<String, String> {
    let (mut rx, _) = app.shell().sidecar("yt-dlp")
        .map_err(|e| e.to_string())?.args(["--version"]).spawn().map_err(|e| e.to_string())?;
    while let Some(event) = rx.recv().await {
        match event {
            CommandEvent::Stdout(b) => return Ok(String::from_utf8_lossy(&b).trim().to_string()),
            CommandEvent::Terminated(_) => break,
            _ => {}
        }
    }
    Err("Could not read version".to_string())
}

#[tauri::command]
async fn update_ytdlp(app: AppHandle) -> Result<String, String> {
    let (mut rx, _) = app.shell().sidecar("yt-dlp")
        .map_err(|e| e.to_string())?.args(["-U"]).spawn().map_err(|e| e.to_string())?;
    let mut out = String::new();
    let mut exit_ok = false;
    while let Some(event) = rx.recv().await {
        match event {
            CommandEvent::Stdout(b) | CommandEvent::Stderr(b) => out += &String::from_utf8_lossy(&b),
            CommandEvent::Terminated(status) => { exit_ok = status.code == Some(0); break; }
            _ => {}
        }
    }
    let out = out.trim().to_string();
    if exit_ok {
        return Ok(out);
    }
    // yt-dlp's self-update rewrites its own binary in place. The most common
    // failure mode is that Catalyst is installed somewhere the current user
    // can't write to (Program Files, /Applications, etc). Surface that instead
    // of a bare non-zero exit code — previously this always returned Ok(), so
    // callers had to guess success/failure by sniffing the word "error" in the
    // combined stdout/stderr, which silently misclassified real failures.
    let hint = "yt-dlp couldn't update itself — this often means Catalyst is installed \
                somewhere that needs admin rights to write to. Try running Catalyst as \
                administrator once, or reinstall it somewhere you have write access.";
    Err(if out.is_empty() { hint.to_string() } else { format!("{hint}\n\n{out}") })
}

#[tauri::command]
fn get_app_version() -> &'static str { APP_VERSION }

/// Version info for the About tab. In dev builds (`cargo tauri dev` /
/// `debug_assertions`), the commit hash/date is more useful than the crate
/// version — the latter doesn't change between local rebuilds, so it's easy
/// to lose track of whether you're actually running the build you think you
/// are. Release builds still show the plain semver.
#[derive(serde::Serialize)]
struct BuildInfo {
    version: String,
    is_dev: bool,
    /// Short commit hash this binary was built from ("unknown" if git wasn't
    /// available at build time — e.g. a source tarball without .git).
    commit_hash: String,
    /// ISO 8601 commit date/time ("unknown" if unavailable) — left
    /// unformatted so the frontend can render it in the user's locale.
    commit_date: String,
}

#[tauri::command]
fn get_build_info() -> BuildInfo {
    BuildInfo {
        version: APP_VERSION.to_string(),
        is_dev: cfg!(debug_assertions),
        commit_hash: env!("CATALYST_GIT_HASH").to_string(),
        commit_date: env!("CATALYST_GIT_DATE").to_string(),
    }
}

// ─── catalyst update check ───────────────────────────────────────────────────

#[tauri::command]
async fn check_for_catalyst_update(force: Option<bool>, state: State<'_, AppStateRef>) -> Result<Option<String>, String> {
    if force != Some(true) {
        if let Some(v) = state.update_available.lock().unwrap().clone() {
            return Ok(Some(v));
        }
    }
    let latest = do_update_check().await;
    *state.update_available.lock().unwrap() = latest.clone();
    Ok(latest)
}

#[tauri::command]
fn get_update_available(state: State<'_, AppStateRef>) -> Option<String> {
    state.update_available.lock().unwrap().clone()
}

// ─── crash/restart recovery ──────────────────────────────────────────────────

/// Read-and-reset how many downloads were auto-resumed from a queue
/// snapshot this startup, so the frontend can show a one-time notice.
#[tauri::command]
fn take_resumed_on_startup(state: State<'_, AppStateRef>) -> u32 {
    std::mem::take(&mut *state.resumed_on_startup.lock().unwrap())
}

// ─── autostart ───────────────────────────────────────────────────────────────

#[tauri::command]
fn get_autostart(app: AppHandle) -> bool {
    use tauri_plugin_autostart::ManagerExt;
    app.autolaunch().is_enabled().unwrap_or(false)
}

#[tauri::command]
fn set_autostart(enabled: bool, app: AppHandle) -> Result<(), String> {
    use tauri_plugin_autostart::ManagerExt;
    if enabled { app.autolaunch().enable().map_err(|e| e.to_string()) }
    else        { app.autolaunch().disable().map_err(|e| e.to_string()) }
}

// ─── setup ───────────────────────────────────────────────────────────────────

pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_notification::init())
        .plugin(tauri_plugin_autostart::init(tauri_plugin_autostart::MacosLauncher::LaunchAgent, None))
        .setup(|app| {
            let cfg = load_config(app.handle());

            // System tray
            let show  = MenuItem::with_id(app, "show",  "Show Catalyst", true, None::<&str>)?;
            let quit  = MenuItem::with_id(app, "quit",  "Quit",          true, None::<&str>)?;
            let menu  = Menu::with_items(app, &[&show, &quit])?;
            let _tray = TrayIconBuilder::new()
                .icon(app.default_window_icon().unwrap().clone())
                .tooltip("Catalyst")
                .menu(&menu)
                .show_menu_on_left_click(false)
                .on_menu_event(|app, event| match event.id.as_ref() {
                    "show" => {
                        if let Some(w) = app.get_webview_window("main") {
                            let _ = w.show(); let _ = w.set_focus();
                        }
                    }
                    "quit" => app.exit(0),
                    _ => {}
                })
                .on_tray_icon_event(|tray, event| {
                    if let TrayIconEvent::Click { button: MouseButton::Left, button_state: MouseButtonState::Up, .. } = event {
                        let app = tray.app_handle();
                        if let Some(w) = app.get_webview_window("main") {
                            if w.is_visible().unwrap_or(false) && w.is_focused().unwrap_or(false) {
                                let _ = w.hide();
                            } else {
                                let _ = w.show(); let _ = w.set_focus();
                            }
                        }
                    }
                })
                .build(app)?;

            // Close behaviour — check minimize_to_tray config each time
            let handle = app.handle().clone();
            app.get_webview_window("main").unwrap().on_window_event(move |event| {
                if let tauri::WindowEvent::CloseRequested { api, .. } = event {
                    let minimize = handle.try_state::<AppStateRef>()
                        .map(|s| s.config.lock().unwrap().minimize_to_tray)
                        .unwrap_or(true);
                    if minimize {
                        if let Some(w) = handle.get_webview_window("main") { let _ = w.hide(); }
                        api.prevent_close();
                    }
                    // else: allow close → Tauri exits when last window closes
                }
            });

            // Background yt-dlp auto-update
            if cfg.auto_update_ytdlp {
                let h = app.handle().clone();
                tauri::async_runtime::spawn(async move {
                    let _ = h.shell().sidecar("yt-dlp").ok().and_then(|s| s.args(["-U"]).spawn().ok());
                });
            }

            // Background update check
            let check_updates = cfg.auto_check_updates;
            let database = init_db(app.handle());
            let state_arc = Arc::new(AppState::new(cfg, database));
            app.manage(state_arc.clone());

            if check_updates {
                let check_state = state_arc.clone();
                tauri::async_runtime::spawn(async move {
                    if let Some(v) = do_update_check().await {
                        *check_state.update_available.lock().unwrap() = Some(v);
                    }
                });
            }

            // ─── crash/restart recovery ──────────────────────────────────
            // Restore whatever was in the queue when the app last closed
            // (see the periodic snapshot task below). Jobs that already
            // reached a terminal state (Finished/Failed/Cancelled) are just
            // redisplayed as-is; anything still active — Fetching, Queued,
            // Downloading, or Processing — was interrupted mid-flight, so
            // reset it to Queued and re-enqueue it. yt-dlp resumes a partial
            // file by default when given the same output path, so this
            // isn't just "start over" for a partway-downloaded file.
            if let Some(db) = state_arc.db.as_ref() {
                if let Ok(snapshot) = db.load_queue_snapshot() {
                    let mut to_resume: Vec<DownloadJob> = Vec::new();
                    {
                        let mut jobs = state_arc.jobs.lock().unwrap();
                        for mut job in snapshot {
                            let was_active = matches!(job.status,
                                DownloadStatus::Fetching | DownloadStatus::Queued
                                | DownloadStatus::Downloading | DownloadStatus::Processing);
                            if was_active {
                                job.status = DownloadStatus::Queued;
                                job.progress = 0.0; job.speed = None; job.eta = None;
                                to_resume.push(job.clone());
                            }
                            jobs.push(job);
                        }
                    }
                    if !to_resume.is_empty() {
                        *state_arc.resumed_on_startup.lock().unwrap() = to_resume.len() as u32;
                        let handle = app.handle().clone();
                        for job in to_resume {
                            enqueue(job.id, job.url, job.format_type, job.quality, job.category_id, &state_arc, handle.clone());
                        }
                    }
                }
            }

            // Periodic snapshot of the active queue so an unexpected
            // shutdown (crash, force-quit, PC restart) doesn't silently lose
            // track of in-progress downloads. A timer rather than a write on
            // every mutation — this is a recovery checkpoint, not a source
            // of truth, so a few seconds of staleness on a hard crash is an
            // acceptable trade-off for not threading a DB write through
            // every progress tick in worker.rs.
            {
                let snapshot_state = state_arc.clone();
                tauri::async_runtime::spawn(async move {
                    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(5));
                    loop {
                        interval.tick().await;
                        let jobs = snapshot_state.jobs.lock().unwrap().clone();
                        if let Some(db) = snapshot_state.db.as_ref() {
                            let _ = db.save_queue_snapshot(&jobs);
                        }
                    }
                });
            }

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            add_download, add_downloads_bulk, get_queue,
            cancel_download, retry_download,
            remove_job, remove_jobs, clear_completed, reorder_queue,
            open_folder, open_url, delete_file, read_text_file,
            set_queue_paused, get_queue_paused, get_auto_pause_reason,
            get_config, save_config,
            get_history, delete_history_entry, clear_history, get_history_stats,
            set_history_pause, get_history_pause,
            detect_browsers,
            get_ytdlp_version, update_ytdlp, get_app_version, get_build_info,
            check_for_catalyst_update, get_update_available,
            get_autostart, set_autostart,
            take_resumed_on_startup,
        ])
        .run(tauri::generate_context!())
        .expect("error while running Catalyst");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_info_reflects_debug_assertions_and_has_nonempty_fields() {
        let info = get_build_info();
        assert_eq!(info.is_dev, cfg!(debug_assertions));
        assert_eq!(info.version, APP_VERSION);
        // "unknown" is an acceptable fallback (e.g. building without git
        // available), but the fields must never be empty — the frontend
        // always has something to show.
        assert!(!info.commit_hash.is_empty());
        assert!(!info.commit_date.is_empty());
    }
}
