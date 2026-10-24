use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use axum::{
    body::Body,
    extract::{DefaultBodyLimit, Multipart, Path, State},
    http::{HeaderMap, Request, StatusCode},
    middleware::{self, Next},
    response::{Html, IntoResponse, Json, Response},
    routing::{get, post, put},
    Router,
};
use percent_encoding::percent_decode_str;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use tokio::{
    io::AsyncWriteExt,
    process::Command,
    sync::{mpsc, Mutex},
};
use tower_http::{cors::CorsLayer, services::ServeDir, trace::TraceLayer};
use uuid::Uuid;

// ---- config -----------------------------------------------------------
// Keep this generous but bounded: on a 1GB box you don't want an
// unbounded upload to be able to exhaust disk silently. Adjust to taste.
const MAX_UPLOAD_BYTES: usize = 4 * 1024 * 1024 * 1024; // 4GB per file

#[derive(Clone)]
struct AppState {
    db: Arc<Mutex<Connection>>,
    upload_dir: PathBuf,
    // Hands off 720p transcode jobs to the background worker. Unbounded is
    // fine here: jobs are tiny (an id/slug/filename), the worker drains
    // them one at a time, and uploads are nowhere near frequent enough on
    // a personal box to need backpressure.
    transcode_tx: mpsc::UnboundedSender<TranscodeJob>,
}

#[derive(Serialize, Clone)]
struct VideoEntry {
    id: String,
    original_name: String,
    stored_name: String,
    slug: String,
    size_bytes: i64,
    views: i64,
    // None until the background worker finishes (or decides the source is
    // already <=720p and skips it). `transcode_status` tells the frontend
    // which of those cases it's in so it knows whether to show a quality
    // toggle, a "transcoding…" label, or nothing.
    stored_name_720p: Option<String>,
    transcode_status: String, // "pending" | "done" | "skipped" | "failed"
    uploaded_at: String,
}

#[derive(Deserialize)]
struct SlugUpdate {
    slug: String,
}

/// A unit of work for the background transcode worker: "make a 720p copy
/// of this upload." Carries the slug (not just the id) because the 720p
/// file's name is derived from the slug, same as the original.
struct TranscodeJob {
    id: String,
    slug: String,
    stored_name: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    // ffmpeg/ffprobe are separate binaries the code shells out to, not Rust
    // dependencies, so a missing install won't show up as a build error —
    // it'd otherwise fail silently per-upload. Check once at startup and
    // warn loudly so it's obvious instead.
    check_ffmpeg_tool("ffmpeg", "the faststart fix (moov atom relocation) and all 720p transcoding").await;
    check_ffmpeg_tool("ffprobe", "skipping 720p transcodes for videos that are already <=720p (everything will get re-encoded instead, which still works, just burns more CPU/disk than necessary)").await;

    let upload_dir = PathBuf::from("uploads");
    tokio::fs::create_dir_all(&upload_dir).await?;

    let db = Connection::open("media.db")?;
    db.execute_batch(
        "CREATE TABLE IF NOT EXISTS videos (
            id               TEXT PRIMARY KEY,
            original_name    TEXT NOT NULL,
            stored_name      TEXT NOT NULL,
            size_bytes       INTEGER NOT NULL,
            views            INTEGER NOT NULL DEFAULT 0,
            stored_name_720p TEXT,
            transcode_status TEXT NOT NULL DEFAULT 'skipped',
            uploaded_at      TEXT NOT NULL DEFAULT (datetime('now'))
        );",
    )?;

    // Migration: add `slug` to databases created before share URLs existed.
    // SQLite has no "ADD COLUMN IF NOT EXISTS", so check first.
    let has_slug: bool = db
        .prepare("SELECT 1 FROM pragma_table_info('videos') WHERE name = 'slug'")?
        .exists([])?;
    if !has_slug {
        db.execute_batch("ALTER TABLE videos ADD COLUMN slug TEXT;")?;
        // Backfill any existing rows with a slug derived from their id so
        // old links don't all collide on NULL/empty.
        let mut stmt = db.prepare("SELECT id FROM videos WHERE slug IS NULL")?;
        let ids: Vec<String> = stmt
            .query_map([], |r| r.get::<_, String>(0))?
            .filter_map(|r| r.ok())
            .collect();
        drop(stmt);
        for id in ids {
            let fallback = default_slug(&id);
            db.execute(
                "UPDATE videos SET slug = ?1 WHERE id = ?2",
                rusqlite::params![fallback, id],
            )?;
        }
    }
    db.execute_batch(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_videos_slug ON videos(slug);",
    )?;

    // Migration: add `views` to databases created before view counts existed.
    let has_views: bool = db
        .prepare("SELECT 1 FROM pragma_table_info('videos') WHERE name = 'views'")?
        .exists([])?;
    if !has_views {
        db.execute_batch("ALTER TABLE videos ADD COLUMN views INTEGER NOT NULL DEFAULT 0;")?;
    }

    // Migration: add 720p columns to databases created before resolution
    // switching existed. Existing rows default to 'skipped' (not
    // 'pending') — we don't want every video you've ever uploaded to
    // suddenly queue up a transcode the moment you restart the server.
    // Re-trigger individual videos manually later if you want them to
    // get a 720p copy retroactively.
    let has_720p: bool = db
        .prepare("SELECT 1 FROM pragma_table_info('videos') WHERE name = 'stored_name_720p'")?
        .exists([])?;
    if !has_720p {
        db.execute_batch(
            "ALTER TABLE videos ADD COLUMN stored_name_720p TEXT;
             ALTER TABLE videos ADD COLUMN transcode_status TEXT NOT NULL DEFAULT 'skipped';",
        )?;
    }

    let db = Arc::new(Mutex::new(db));

    let (transcode_tx, transcode_rx) = mpsc::unbounded_channel::<TranscodeJob>();
    tokio::spawn(transcode_worker(transcode_rx, db.clone(), upload_dir.clone()));

    let state = AppState {
        db,
        upload_dir: upload_dir.clone(),
        transcode_tx,
    };

    // /files/* serves the raw uploaded files straight off disk.
    // tower-http's ServeDir handles Range requests for us, which is
    // what lets <video> seek/scrub instead of only playing start-to-end.
    let files_service = ServeDir::new(&upload_dir);

    // /* serves the dashboard (static/index.html + any assets in static/)
    let static_service = ServeDir::new("static");

    let app = Router::new()
        .route("/api/upload", post(upload_handler))
        .route("/api/videos", get(list_handler))
        .route("/api/videos/:id", axum::routing::delete(delete_handler))
        .route("/api/videos/:id/slug", put(update_slug_handler))
        .route("/api/videos/by-slug/:slug", get(get_by_slug_handler))
        .route("/w/:slug", get(watch_page_handler))
        .nest_service("/files", files_service)
        .fallback_service(static_service)
        .layer(DefaultBodyLimit::max(MAX_UPLOAD_BYTES))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            view_count_middleware,
        ))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    tracing::info!("listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

/// Runs `<tool> -version`/`-version` to confirm it's on PATH, logging a
/// warning naming exactly what breaks if it's missing instead of letting
/// that surface later as a silent per-upload failure.
async fn check_ffmpeg_tool(tool: &str, what_it_powers: &str) {
    match Command::new(tool).arg("-version").output().await {
        Ok(out) if out.status.success() => {
            let version_line = String::from_utf8_lossy(&out.stdout)
                .lines()
                .next()
                .unwrap_or(tool)
                .to_string();
            tracing::info!("found {version_line}");
        }
        _ => {
            tracing::warn!(
                "{tool} not found on PATH — this affects {what_it_powers}. \
                 Install it and make sure it's on PATH, then restart."
            );
        }
    }
}

// ---- slug helpers ---------------------------------------------------------

/// Default share slug for a freshly uploaded file: short, derived from its
/// uuid so it's unique without needing an extra random-generator dependency.
/// The user can rename it to anything they like afterward.
fn default_slug(id: &str) -> String {
    id.replace('-', "").chars().take(10).collect()
}

/// Share slugs become part of a public URL, so keep them boring on purpose:
/// lowercase letters, digits, hyphens, 3-64 chars. No leading/trailing
/// hyphen, no path-traversal-flavored characters.
fn validate_slug(slug: &str) -> Result<(), String> {
    if slug.len() < 3 || slug.len() > 64 {
        return Err("slug must be 3-64 characters".into());
    }
    if !slug
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return Err("slug can only contain lowercase letters, numbers, and hyphens".into());
    }
    if slug.starts_with('-') || slug.ends_with('-') {
        return Err("slug can't start or end with a hyphen".into());
    }
    Ok(())
}

// ---- video processing helpers --------------------------------------------

/// Re-muxes an MP4/MOV file in place with `-movflags +faststart`, moving
/// the "moov atom" (the file's metadata/index) to the front. Browsers can
/// then start playback immediately instead of buffering the whole file,
/// and — the thing that actually matters here — Discord's embed player
/// won't play a video at all if the moov atom is at the end.
///
/// Uses `-c copy`, so this just rewrites the container (fast, lossless),
/// not a real transcode. Shells out to the `ffmpeg` binary rather than
/// pulling in an FFI binding, so it needs `ffmpeg` on PATH on the host.
///
/// Returns the new file size on success. Returns None (leaving the
/// original upload untouched) if the extension isn't MP4-family, ffmpeg
/// isn't installed, or the remux fails for any reason — this is treated
/// as best-effort, not something that should fail the whole upload.
async fn faststart_remux(path: &PathBuf) -> Option<i64> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    if !matches!(ext.as_str(), "mp4" | "mov" | "m4v") {
        return None; // faststart only applies to the MP4/MOV family of containers
    }

    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("video");
    let tmp_path = path.with_file_name(format!("{stem}.faststart.{ext}"));

    let status = Command::new("ffmpeg")
        .args(["-y", "-loglevel", "error", "-i"])
        .arg(&path)
        .args(["-c", "copy", "-movflags", "+faststart"])
        .arg(&tmp_path)
        .status()
        .await
        .ok()?;

    if !status.success() {
        let _ = tokio::fs::remove_file(&tmp_path).await;
        tracing::warn!("ffmpeg faststart remux failed for {path:?}; keeping original upload");
        return None;
    }

    let new_size = tokio::fs::metadata(&tmp_path).await.ok()?.len() as i64;
    tokio::fs::rename(&tmp_path, path).await.ok()?;
    Some(new_size)
}

/// Reads the source video's pixel height via ffprobe. Used to decide
/// whether a 720p copy is even worth making — no point re-encoding (and
/// burning CPU on a 2-OCPU box) a video that's already 480p. Returns None
/// if ffprobe is missing or the probe fails for any reason; callers treat
/// that as "unknown, better to just transcode" rather than blocking.
async fn probe_height(path: &PathBuf) -> Option<u32> {
    let out = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=height",
            "-of",
            "csv=p=0",
        ])
        .arg(path)
        .output()
        .await
        .ok()?;

    if !out.status.success() {
        return None;
    }
    String::from_utf8_lossy(&out.stdout).trim().parse().ok()
}

/// Background worker that drains the transcode queue one job at a time —
/// deliberately serial, not spawned-per-upload, so a batch of uploads
/// doesn't try to run several ffmpeg encodes at once and starve the 2
/// OCPUs this is meant to run on.
async fn transcode_worker(
    mut rx: mpsc::UnboundedReceiver<TranscodeJob>,
    db: Arc<Mutex<Connection>>,
    upload_dir: PathBuf,
) {
    while let Some(job) = rx.recv().await {
        let input_path = upload_dir.join(&job.stored_name);

        // Skip re-encoding videos that are already <=720p — nothing to
        // gain from it, and it'd just burn CPU for an output that's
        // barely different from the original.
        match probe_height(&input_path).await {
            Some(h) if h <= 720 => {
                set_transcode_status(&db, &job.id, "skipped", None).await;
                continue;
            }
            _ => {} // unknown or >720p — go ahead and transcode
        }

        // Always output plain .mp4 regardless of the source container —
        // we're re-encoding anyway, so there's no reason to preserve e.g.
        // a .mkv/.webm extension, and it keeps the 720p file's mime type
        // predictable.
        let output_name = format!("{}-720p.mp4", job.slug);
        let output_path = upload_dir.join(&output_name);

        let status = Command::new("ffmpeg")
            .args(["-y", "-loglevel", "error", "-i"])
            .arg(&input_path)
            .args([
                "-vf", "scale=-2:720",
                "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
                "-c:a", "aac", "-b:a", "128k",
                "-movflags", "+faststart",
            ])
            .arg(&output_path)
            .status()
            .await;

        match status {
            Ok(s) if s.success() => {
                set_transcode_status(&db, &job.id, "done", Some(&output_name)).await;
            }
            other => {
                if let Err(e) = other {
                    tracing::warn!("720p transcode failed to launch for {}: {e}", job.stored_name);
                } else {
                    tracing::warn!("720p transcode failed for {}", job.stored_name);
                }
                let _ = tokio::fs::remove_file(&output_path).await;
                set_transcode_status(&db, &job.id, "failed", None).await;
            }
        }
    }
}

async fn set_transcode_status(
    db: &Arc<Mutex<Connection>>,
    id: &str,
    status: &str,
    stored_name_720p: Option<&str>,
) {
    let db = db.lock().await;
    let _ = db.execute(
        "UPDATE videos SET transcode_status = ?1, stored_name_720p = ?2 WHERE id = ?3",
        rusqlite::params![status, stored_name_720p, id],
    );
}

// ---- view counting ---------------------------------------------------------

/// Counts a "view" for direct hits on /files/<name> — someone opening the
/// raw share link themselves, or Discord/another embed fetching the video
/// bytes to actually play it — without wildly over-counting.
///
/// Two things would inflate this if we counted every matching request:
///   1. A <video> element issues a bunch of small Range requests while
///      seeking/buffering. Only the *first* request for a file (no Range
///      header, or one starting at byte 0) counts as a view.
///   2. The dashboard's own cards use `preload="metadata"`, so just
///      loading the dashboard quietly fetches a few bytes of every video
///      on it. We skip counting when the request's Referer is our own
///      origin — that covers both the dashboard grid and a video played
///      from the /w/:slug page (whose page load is already counted once,
///      separately, in watch_page_handler).
async fn view_count_middleware(
    State(state): State<AppState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let path = req.uri().path();

    if let Some(encoded_name) = path.strip_prefix("/files/") {
        let is_initial_request = match req.headers().get(axum::http::header::RANGE) {
            None => true,
            Some(v) => v
                .to_str()
                .map(|s| s.trim_start_matches("bytes=").starts_with('0'))
                .unwrap_or(false),
        };

        let host = req
            .headers()
            .get(axum::http::header::HOST)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let is_external = req
            .headers()
            .get(axum::http::header::REFERER)
            .and_then(|v| v.to_str().ok())
            .map(|referer| host.is_empty() || !referer.contains(host))
            .unwrap_or(true); // no Referer at all -> typed/pasted link, Discord, curl, etc.

        if is_initial_request && is_external {
            if let Ok(stored_name) = percent_decode_str(encoded_name).decode_utf8() {
                let db = state.db.lock().await;
                // Matches either the original file or its 720p copy — a
                // view of either quality counts as a view of the video.
                let _ = db.execute(
                    "UPDATE videos SET views = views + 1
                     WHERE stored_name = ?1 OR stored_name_720p = ?1",
                    [stored_name.as_ref()],
                );
            }
        }
    }

    next.run(req).await
}

// ---- handlers -----------------------------------------------------------

async fn upload_handler(
    State(state): State<AppState>,
    mut multipart: Multipart,
) -> Result<Json<VideoEntry>, (StatusCode, String)> {
    while let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?
    {
        // We only care about the "file" field; skip anything else
        // (e.g. a future "title" text field from the form).
        if field.name() != Some("file") {
            continue;
        }

        let original_name = field
            .file_name()
            .unwrap_or("upload.bin")
            .to_string();

        let ext = PathBuf::from(&original_name)
            .extension()
            .map(|e| format!(".{}", e.to_string_lossy()))
            .unwrap_or_default();

        let id = Uuid::new_v4().to_string();
        // Filename on disk mirrors the share slug from the start (both are
        // derived from the same uuid at upload time), so /files/<slug>.<ext>
        // matches /w/<slug> immediately without a separate rename step.
        let slug = default_slug(&id);
        let stored_name = format!("{slug}{ext}");
        let dest_path = state.upload_dir.join(&stored_name);

        let mut file = tokio::fs::File::create(&dest_path)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let mut size: i64 = 0;

        // Stream chunk-by-chunk straight to disk. This is the important
        // part on a 1GB instance: we never hold the whole file in memory,
        // just whatever chunk size the client sends.
        while let Some(chunk) = field
            .chunk()
            .await
            .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?
        {
            file.write_all(&chunk)
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            size += chunk.len() as i64;
        }
        file.flush().await.ok();
        drop(file);

        // Best-effort faststart remux so Discord embeds and in-browser
        // playback don't have to buffer the whole file first. If ffmpeg
        // is missing or the remux fails we just keep the original upload.
        if let Some(fixed_size) = faststart_remux(&dest_path).await {
            size = fixed_size;
        }

        let db = state.db.lock().await;
        db.execute(
            "INSERT INTO videos (id, original_name, stored_name, slug, size_bytes, transcode_status)
             VALUES (?1, ?2, ?3, ?4, ?5, 'pending')",
            rusqlite::params![id, original_name, stored_name, slug, size],
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let uploaded_at: String = db
            .query_row(
                "SELECT uploaded_at FROM videos WHERE id = ?1",
                [&id],
                |r| r.get(0),
            )
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        drop(db);

        // Queue the 720p transcode instead of doing it inline — the
        // upload response goes back to the browser immediately, and the
        // worker picks this up once it's done with anything ahead of it.
        let _ = state.transcode_tx.send(TranscodeJob {
            id: id.clone(),
            slug: slug.clone(),
            stored_name: stored_name.clone(),
        });

        return Ok(Json(VideoEntry {
            id,
            original_name,
            stored_name,
            slug,
            size_bytes: size,
            views: 0,
            stored_name_720p: None,
            transcode_status: "pending".to_string(),
            uploaded_at,
        }));
    }

    Err((StatusCode::BAD_REQUEST, "no file field found".into()))
}

async fn list_handler(
    State(state): State<AppState>,
) -> Result<Json<Vec<VideoEntry>>, (StatusCode, String)> {
    let db = state.db.lock().await;
    let mut stmt = db
        .prepare(
            "SELECT id, original_name, stored_name, slug, size_bytes, views,
                    stored_name_720p, transcode_status, uploaded_at
             FROM videos ORDER BY uploaded_at DESC",
        )
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let rows = stmt
        .query_map([], |r| {
            Ok(VideoEntry {
                id: r.get(0)?,
                original_name: r.get(1)?,
                stored_name: r.get(2)?,
                slug: r.get(3)?,
                size_bytes: r.get(4)?,
                views: r.get(5)?,
                stored_name_720p: r.get(6)?,
                transcode_status: r.get(7)?,
                uploaded_at: r.get(8)?,
            })
        })
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let mut out = Vec::new();
    for row in rows {
        out.push(row.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?);
    }
    Ok(Json(out))
}

async fn get_by_slug_handler(
    State(state): State<AppState>,
    Path(slug): Path<String>,
) -> Result<Json<VideoEntry>, (StatusCode, String)> {
    let db = state.db.lock().await;
    db.query_row(
        "SELECT id, original_name, stored_name, slug, size_bytes, views,
                stored_name_720p, transcode_status, uploaded_at
         FROM videos WHERE slug = ?1",
        [&slug],
        |r| {
            Ok(VideoEntry {
                id: r.get(0)?,
                original_name: r.get(1)?,
                stored_name: r.get(2)?,
                slug: r.get(3)?,
                size_bytes: r.get(4)?,
                views: r.get(5)?,
                stored_name_720p: r.get(6)?,
                transcode_status: r.get(7)?,
                uploaded_at: r.get(8)?,
            })
        },
    )
    .map(Json)
    .map_err(|_| (StatusCode::NOT_FOUND, "not found".into()))
}

async fn update_slug_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<SlugUpdate>,
) -> impl IntoResponse {
    let new_slug = body.slug.trim().to_lowercase();

    if let Err(msg) = validate_slug(&new_slug) {
        return (StatusCode::BAD_REQUEST, msg).into_response();
    }

    let db = state.db.lock().await;

    // Is this slug already taken by a *different* video?
    let taken_by_other: bool = db
        .query_row(
            "SELECT 1 FROM videos WHERE slug = ?1 AND id != ?2",
            rusqlite::params![new_slug, id],
            |_| Ok(()),
        )
        .is_ok();
    if taken_by_other {
        return (StatusCode::CONFLICT, "that share URL is already taken".to_string())
            .into_response();
    }

    // Look up the current stored_name (and optional 720p stored name) so
    // we know the on-disk filenames to preserve. Both mirror the slug
    // (<slug><ext> / <slug>-720p.mp4), so renaming the slug means
    // renaming the file(s) too — otherwise /files/<new_slug><ext> would
    // 404 while the old path still has the bytes.
    let old_row: Option<(String, Option<String>)> = db
        .query_row(
            "SELECT stored_name, stored_name_720p FROM videos WHERE id = ?1",
            [&id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .ok();

    let Some((old_stored_name, old_stored_name_720p)) = old_row else {
        return (StatusCode::NOT_FOUND, "video not found".to_string()).into_response();
    };

    let ext = PathBuf::from(&old_stored_name)
        .extension()
        .map(|e| format!(".{}", e.to_string_lossy()))
        .unwrap_or_default();
    let new_stored_name = format!("{new_slug}{ext}");
    let new_stored_name_720p = old_stored_name_720p
        .as_ref()
        .map(|_| format!("{new_slug}-720p.mp4"));

    // Rename on disk first, for both files. If either fails (permissions,
    // disk issue, the file having gone missing), bail out before touching
    // the DB so stored_name never points at a file that doesn't exist. If
    // the second rename fails after the first succeeded, undo the first
    // so disk state doesn't end up half-renamed.
    if new_stored_name != old_stored_name {
        let old_path = state.upload_dir.join(&old_stored_name);
        let new_path = state.upload_dir.join(&new_stored_name);
        if let Err(e) = tokio::fs::rename(&old_path, &new_path).await {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("couldn't rename file on disk: {e}"),
            )
                .into_response();
        }
    }
    if let (Some(old_720p), Some(new_720p)) = (&old_stored_name_720p, &new_stored_name_720p) {
        if old_720p != new_720p {
            let old_path = state.upload_dir.join(old_720p);
            let new_path = state.upload_dir.join(new_720p);
            if let Err(e) = tokio::fs::rename(&old_path, &new_path).await {
                // Put the main file back before bailing, so we don't leave
                // disk and DB disagreeing about the main file either.
                if new_stored_name != old_stored_name {
                    let new_path = state.upload_dir.join(&new_stored_name);
                    let old_path = state.upload_dir.join(&old_stored_name);
                    let _ = tokio::fs::rename(&new_path, &old_path).await;
                }
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("couldn't rename 720p file on disk: {e}"),
                )
                    .into_response();
            }
        }
    }

    let updated = db
        .execute(
            "UPDATE videos SET slug = ?1, stored_name = ?2, stored_name_720p = ?3 WHERE id = ?4",
            rusqlite::params![new_slug, new_stored_name, new_stored_name_720p, id],
        )
        .unwrap_or(0);

    if updated == 0 {
        return (StatusCode::NOT_FOUND, "video not found".to_string()).into_response();
    }

    // Returns the new stored names too, so the dashboard can update its
    // "copy direct link" buttons immediately instead of pointing at the
    // now-renamed-away-from file until the next full reload.
    Json(serde_json::json!({
        "slug": new_slug,
        "stored_name": new_stored_name,
        "stored_name_720p": new_stored_name_720p,
    }))
    .into_response()
}

async fn delete_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let db = state.db.lock().await;

    let row: Option<(String, Option<String>)> = db
        .query_row(
            "SELECT stored_name, stored_name_720p FROM videos WHERE id = ?1",
            [&id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .ok();

    let Some((stored_name, stored_name_720p)) = row else {
        return (StatusCode::NOT_FOUND, "not found").into_response();
    };

    let _ = db.execute("DELETE FROM videos WHERE id = ?1", [&id]);
    let _ = tokio::fs::remove_file(state.upload_dir.join(stored_name)).await;
    if let Some(name_720p) = stored_name_720p {
        let _ = tokio::fs::remove_file(state.upload_dir.join(name_720p)).await;
    }

    StatusCode::NO_CONTENT.into_response()
}

/// Renders the /w/:slug share page server-side, with the video's data
/// baked straight into the HTML (including Open Graph / Twitter Player
/// meta tags). This matters because Discord's link-preview crawler, and
/// most embeddable-player tools, read <meta> tags from the raw HTML
/// response — they never execute JavaScript. A page that only fills
/// itself in via a client-side fetch() looks empty to them, so there's
/// nothing to embed. The embed always points at the *original* file,
/// never the 720p copy — Discord gets the best quality you uploaded.
async fn watch_page_handler(
    State(state): State<AppState>,
    Path(slug): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let db = state.db.lock().await;
    let mut video = db.query_row(
        "SELECT id, original_name, stored_name, slug, size_bytes, views,
                stored_name_720p, transcode_status, uploaded_at
         FROM videos WHERE slug = ?1",
        [&slug],
        |r| {
            Ok(VideoEntry {
                id: r.get(0)?,
                original_name: r.get(1)?,
                stored_name: r.get(2)?,
                slug: r.get(3)?,
                size_bytes: r.get(4)?,
                views: r.get(5)?,
                stored_name_720p: r.get(6)?,
                transcode_status: r.get(7)?,
                uploaded_at: r.get(8)?,
            })
        },
    );

    // Count this page load as a view. Unlike /files/ (hit repeatedly by
    // Range requests), this handler runs exactly once per link-open —
    // whether that's a person following the share link or Discord's
    // crawler building the embed preview — so no dedup logic is needed.
    if let Ok(ref mut v) = video {
        if db
            .execute("UPDATE videos SET views = views + 1 WHERE slug = ?1", [&slug])
            .is_ok()
        {
            v.views += 1;
        }
    }
    drop(db);

    let Ok(video) = video else {
        return (StatusCode::NOT_FOUND, Html(render_not_found_html())).into_response();
    };

    let (scheme, host) = request_origin(&headers);
    Html(render_watch_html(&video, &scheme, &host)).into_response()
}

/// Figures out scheme+host for building absolute URLs (needed for
/// og:video — relative URLs don't work in meta tags). Caddy sets
/// X-Forwarded-Proto when reverse-proxying; we fall back to http for
/// plain local testing (localhost/127.0.0.1) and https otherwise, since
/// a real domain in production is assumed to be behind TLS.
fn request_origin(headers: &HeaderMap) -> (String, String) {
    let host = headers
        .get(axum::http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost:3000")
        .to_string();

    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            if host.starts_with("localhost") || host.starts_with("127.0.0.1") {
                "http".to_string()
            } else {
                "https".to_string()
            }
        });

    (scheme, host)
}

// A small inline favicon (clapperboard emoji as an SVG data URI) so every
// page gets a tab icon without needing a static asset file on disk.
const FAVICON_TAG: &str = r#"<link rel="icon" href="data:image/svg+xml,<svg xmlns=%22http://www.w3.org/2000/svg%22 viewBox=%220 0 100 100%22><text y=%22.9em%22 font-size=%2290%22>%F0%9F%8E%AC</text></svg>">"#;

fn render_watch_html(video: &VideoEntry, scheme: &str, host: &str) -> String {
    let video_url = format!(
        "{scheme}://{host}/files/{}",
        urlencode_path_segment(&video.stored_name)
    );
    let page_url = format!("{scheme}://{host}/w/{}", urlencode_path_segment(&video.slug));
    let mime_type = mime_guess::from_path(&video.stored_name)
        .first_or_octet_stream()
        .to_string();
    let title = escape_html(&video.original_name);

    // Quality toggle markup: only rendered when a 720p copy actually
    // exists. While it's still transcoding, a lightweight poll checks
    // back every few seconds and reloads the page once it's ready (or
    // gives up quietly if it ends up skipped/failed).
    let quality_block = match (&video.stored_name_720p, video.transcode_status.as_str()) {
        (Some(name_720p), _) => {
            let url_720p = format!(
                "{scheme}://{host}/files/{}",
                urlencode_path_segment(name_720p)
            );
            format!(
                r#"<select class="quality-select" id="qualitySelect" onchange="switchQuality(this.value)">
      <option value="orig">Original</option>
      <option value="720p">720p</option>
    </select>
    <script>const QUALITY_URLS = {{ orig: {video_url_js}, "720p": {url_720p_js} }};</script>"#,
                video_url_js = serde_json::to_string(&video_url).unwrap_or_default(),
                url_720p_js = serde_json::to_string(&url_720p).unwrap_or_default(),
            )
        }
        (None, "pending") => {
            r#"<span class="transcoding">720p copy processing…</span>
    <script>setTimeout(() => location.reload(), 5000);</script>"#
                .to_string()
        }
        _ => String::new(),
    };

    format!(
        r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>{title}</title>
{FAVICON_TAG}

<!-- Open Graph video embed, read by Discord/Slack/etc. without JS.
     Always points at the original file, never the 720p copy. -->
<meta property="og:type" content="video.other" />
<meta property="og:title" content="{title}" />
<meta property="og:url" content="{page_url}" />
<meta property="og:video" content="{video_url}" />
<meta property="og:video:secure_url" content="{video_url}" />
<meta property="og:video:type" content="{mime_type}" />
<meta name="twitter:card" content="player" />
<meta name="twitter:player:stream" content="{video_url}" />
<meta name="twitter:player:stream:content_type" content="{mime_type}" />

<style>
  :root {{ color-scheme: dark; }}
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0;
    min-height: 100vh;
    display: flex;
    align-items: center;
    justify-content: center;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    background: #0c0d0f;
    color: #e8e8ea;
    padding: 20px;
  }}
  .wrap {{ width: 100%; max-width: 900px; }}
  video {{
    width: 100%;
    max-height: 80vh;
    border-radius: 10px;
    background: #000;
    display: block;
  }}
  h1 {{
    font-size: 15px;
    font-weight: 500;
    margin: 14px 2px 4px;
    word-break: break-word;
  }}
  .meta {{ font-size: 12px; color: #8a8a90; margin: 0 2px; }}
  .row {{ display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }}
  .copy-btn, .quality-select {{
    appearance: none;
    border: 1px solid #2a2a2e;
    background: #17181b;
    color: #e8e8ea;
    font-size: 12px;
    padding: 6px 10px;
    border-radius: 6px;
    cursor: pointer;
    display: inline-flex;
    align-items: center;
    gap: 6px;
  }}
  .copy-btn:hover {{ background: #1f2023; }}
  .copy-btn.copied {{ border-color: #3a7a4a; color: #7fd996; }}
  .transcoding {{ font-size: 12px; color: #8a8a90; }}
  .hint {{ font-size: 11px; color: #5c5d62; margin: 8px 2px 0; }}
</style>
</head>
<body>
  <div class="wrap">
    <video controls autoplay playsinline src="{video_url}" id="player"></video>
    <div class="row">
      <h1 style="flex:1; min-width:0;">{title}</h1>
      {quality_block}
      <button class="copy-btn" id="copyBtn" type="button" onclick="copyDirectLink()">
        <span id="copyLabel">Copy direct link</span>
      </button>
    </div>
    <div class="meta">{size} · {views_label} · uploaded {uploaded_at}</div>
    <div class="hint">← / → seek 5s · space play/pause</div>
  </div>
  <script>
    function switchQuality(which) {{
      const player = document.getElementById('player');
      const wasPaused = player.paused;
      const t = player.currentTime;
      player.src = QUALITY_URLS[which];
      player.currentTime = t;
      if (!wasPaused) player.play();
    }}

    function copyDirectLink() {{
      const url = document.getElementById('player').currentSrc || {video_url_js};
      const btn = document.getElementById('copyBtn');
      const label = document.getElementById('copyLabel');
      const done = () => {{
        label.textContent = 'Copied!';
        btn.classList.add('copied');
        setTimeout(() => {{
          label.textContent = 'Copy direct link';
          btn.classList.remove('copied');
        }}, 1500);
      }};
      if (navigator.clipboard && window.isSecureContext) {{
        navigator.clipboard.writeText(url).then(done).catch(() => fallbackCopy(url, done));
      }} else {{
        fallbackCopy(url, done);
      }}
    }}
    function fallbackCopy(text, done) {{
      const ta = document.createElement('textarea');
      ta.value = text;
      ta.style.position = 'fixed';
      ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.select();
      try {{ document.execCommand('copy'); done(); }} catch (e) {{}}
      document.body.removeChild(ta);
    }}

    // Arrow-key seeking, YouTube/Discord-style. Ignored while the user is
    // typing/selecting into some other control on this page.
    const player = document.getElementById('player');
    document.addEventListener('keydown', (e) => {{
      if (['INPUT', 'TEXTAREA', 'SELECT'].includes(e.target.tagName)) return;
      if (e.key === 'ArrowLeft') {{
        player.currentTime = Math.max(0, player.currentTime - 5);
        e.preventDefault();
      }} else if (e.key === 'ArrowRight') {{
        player.currentTime = Math.min(player.duration || Infinity, player.currentTime + 5);
        e.preventDefault();
      }} else if (e.key === ' ') {{
        player.paused ? player.play() : player.pause();
        e.preventDefault();
      }}
    }});
  </script>
</body>
</html>
"#,
        size = format_bytes(video.size_bytes),
        views_label = format_views(video.views),
        uploaded_at = escape_html(&video.uploaded_at),
        video_url_js = serde_json::to_string(&video_url).unwrap_or_else(|_| "\"\"".to_string()),
    )
}

fn render_not_found_html() -> String {
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Not found</title>
{FAVICON_TAG}
<style>
  body {{
    margin: 0; min-height: 100vh; display: flex; align-items: center;
    justify-content: center; font-family: -apple-system, BlinkMacSystemFont, sans-serif;
    background: #0c0d0f; color: #8a8a90; font-size: 14px;
  }}
</style>
</head>
<body>This link doesn't point to a video.</body>
</html>
"#
    )
}

fn format_bytes(n: i64) -> String {
    let units = ["B", "KB", "MB", "GB"];
    let mut n = n as f64;
    let mut i = 0;
    while n >= 1024.0 && i < units.len() - 1 {
        n /= 1024.0;
        i += 1;
    }
    format!("{:.1} {}", n, units[i])
}

fn format_views(n: i64) -> String {
    format!("{n} view{}", if n == 1 { "" } else { "s" })
}

fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Path segments here are always our own uuid-based stored_name or a
/// validated slug (see validate_slug), so this just guards against any
/// stray characters ending up unescaped inside an HTML attribute/URL.
fn urlencode_path_segment(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '~') {
                c.to_string()
            } else {
                format!("%{:02X}", c as u32)
            }
        })
        .collect()
}
