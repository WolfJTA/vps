# media-server

Minimal axum-based video server: a dashboard to upload files, serves them
back with proper HTTP range support (so `<video>` seeking works). No
transcoding — files are served exactly as uploaded.

## What's here

- `src/main.rs` — the whole server. Routes:
  - `GET /` and static assets → `static/` (the dashboard)
  - `POST /api/upload` → multipart upload, streamed straight to disk (never buffered fully in memory)
  - `GET /api/videos` → JSON list of uploaded videos
  - `DELETE /api/videos/:id` → removes the file + its DB row
  - `GET /files/<stored_name>` → serves the raw file with range-request support
- `static/index.html` — single-page dashboard: drag/drop upload with progress bars, grid of videos with inline `<video>` players and delete buttons
- SQLite (`media.db`, created automatically) tracks `id`, `original_name`, `stored_name`, `size_bytes`, `uploaded_at`
- Uploaded files land in `uploads/`, named `<uuid>.<ext>` so there's no filename-collision or path-traversal risk

## Build

```bash
cargo build --release
```

Binary ends up at `target/release/media-server`. The `Cargo.toml` is set to
`lto = true`, `codegen-units = 1`, `strip = true` — smaller binary, a bit
more RAM-friendly, worth it on a 1GB instance. First build is slower
because of that; rebuilds after a small code change are fast.

## Run it directly (quick test)

```bash
./target/release/media-server
```

Listens on `0.0.0.0:3000`. Visit `http://<server-ip>:3000`.

## Deploying on the Oracle instance

1. **Copy the project over** (or `git clone` if you push it somewhere):
   ```bash
   scp -r media-server/ opc@<your-ip>:~/
   ```
   If you're on an Ampere A1 (ARM) instance, build *on* the instance rather
   than cross-compiling — simplest path, and `cargo build --release` on a
   4-core Ampere free-tier shape is plenty fast. On the 1-OCPU x86 Micro
   shape it'll be slower but still fine, just budget a few minutes.

2. **Install Rust on the instance** if it's not there:
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   source "$HOME/.cargo/env"
   ```

3. **Build:**
   ```bash
   cd media-server
   cargo build --release
   ```

4. **Run it as a systemd service** so it survives reboots/crashes. Create
   `/etc/systemd/system/media-server.service`:
   ```ini
   [Unit]
   Description=media-server
   After=network.target

   [Service]
   Type=simple
   User=opc
   WorkingDirectory=/home/opc/media-server
   ExecStart=/home/opc/media-server/target/release/media-server
   Restart=on-failure
   RestartSec=2
   # Keep it well under the box's total RAM as a safety net
   MemoryMax=700M

   [Install]
   WantedBy=multi-user.target
   ```
   Then:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable --now media-server
   sudo systemctl status media-server
   ```

5. **Put Caddy in front of it** for TLS + your domain. Install Caddy, then
   `/etc/caddy/Caddyfile`:
   ```
   your-domain.com {
       reverse_proxy localhost:3000
   }
   ```
   `sudo systemctl reload caddy`. Caddy auto-provisions a Let's Encrypt
   cert on first request — no manual cert setup needed, as long as:
   - the domain's A record already points at the instance's **reserved**
     public IP, and
   - ports 80 and 443 are open on both the VCN security list/NSG *and*
     the instance's own firewall (`firewalld`/`iptables` — Oracle images
     often have this on by default, separately from the VCN rules).

## Notes / things to watch on a 1GB instance

- `MAX_UPLOAD_BYTES` in `main.rs` caps a single upload at 4GB — purely a
  safety limit against runaway requests, not a memory concern (uploads
  stream to disk in chunks regardless of file size). Lower it if you want
  to be stricter.
- The upload path never does `Vec<u8>`-style buffering of a whole file —
  it writes each multipart chunk to disk as it arrives. That's the part
  that matters for not OOMing on a small instance.
- If you ever add transcoding, that's a different story — `ffmpeg` will
  want real CPU and RAM, and a 1GB Micro instance will struggle with it.
  Keep this server direct-play-only unless you upgrade the shape.
- SQLite is a single file (`media.db`) — back it up alongside `uploads/`
  if you care about not losing the dashboard's metadata.
