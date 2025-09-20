use std::env;
use std::fs;
use std::io;
use std::io::BufRead;
use std::path::Path;

use anyhow::Context;
use futures::future;
use futures::stream;
use futures::stream::TryStreamExt;
use tokio::time;

fn load_urls_from_file(
    path: impl AsRef<Path>,
) -> io::Result<impl Iterator<Item = io::Result<String>>> {
    let lines = fs::OpenOptions::new()
        .read(true)
        .open(path)
        .map(io::BufReader::new)?
        .lines();
    Ok(lines)
}

async fn load_url(url: String) -> anyhow::Result<()> {
    reqwest::get(&url)
        .await?
        .bytes()
        .await
        .map(|body| println!("Downloaded {} bytes from {url}", body.len()))?;
    Ok(())
}

async fn load_urls_concurrent(
    path: impl AsRef<Path>,
    limit: impl Into<Option<usize>>,
) -> anyhow::Result<()> {
    let start = time::Instant::now();
    load_urls_from_file(path)
        .map(stream::iter)?
        .err_into()
        .try_filter(|line| future::ready(!line.is_empty() && !line.starts_with('#')))
        .try_for_each_concurrent(limit, load_url)
        .await
        .map(|_| println!("Loading took {:?}", start.elapsed()))?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url_file = env::args().nth(1).context("No URL file provided")?;
    // Loading URLs one by one
    load_urls_concurrent(&url_file, 1).await?;
    // Loading URLs concurrently with no limit
    load_urls_concurrent(&url_file, None).await?;
    // Loading URLs concurrently with a limit of 10 at a time
    load_urls_concurrent(&url_file, 10).await?;

    Ok(())
}
