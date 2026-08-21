//! Serves the dashboard (plain HTML/CSS/vanilla JS, no build step) directly
//! from the compiled binary via rust-embed (FR-4.1).

use axum::extract::Path;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use rust_embed::RustEmbed;

// Path is relative to this crate's Cargo.toml (crates/api/), so ../../static
// points at the workspace-root static/ folder.
#[derive(RustEmbed)]
#[folder = "../../static/"]
struct StaticAssets;

pub async fn serve_index() -> Response {
    serve_embedded("index.html")
}

pub async fn serve_static(Path(path): Path<String>) -> Response {
    serve_embedded(&path)
}

fn serve_embedded(path: &str) -> Response {
    match StaticAssets::get(path) {
        Some(content) => {
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            (
                [(header::CONTENT_TYPE, mime.as_ref().to_string())],
                content.data.into_owned(),
            )
                .into_response()
        }
        None => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}
