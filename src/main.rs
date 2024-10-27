use std::{path::{Path, PathBuf}, time::SystemTime};
use jdt;
use serde::{Serialize, Deserialize};
use clap::crate_name;
use chrono::{NaiveDate, NaiveDateTime, Local, TimeZone};
use nom_exif::{AsyncMediaParser, AsyncMediaSource, ExifIter, ExifTag};
use tokio::task;
use image::{self, GenericImageView};
use dirs::cache_dir;
use md5;
use anyhow::Result;
use thiserror;
use junk_file;
use async_stream::stream;
use futures::StreamExt;
use num_cpus;

#[derive(thiserror::Error, Debug)]
enum Error {
    #[error("Failed to get exif value: {0} {1}")]
    ExifValueError(PathBuf, String),
    #[error("Failed to get exif time: {0} {1} {2}")]
    ExifTimeError(PathBuf, String, String),
    #[error("No creation date found: {0}")]
    NoCreationDateError(PathBuf),
    #[error("Failed to convert system time to local time: {0}")]
    SystemTimeError(String),
    #[error("Failed to get cache dir")]
    CacheDirError,
}

#[derive(Serialize, Deserialize, Debug)]
struct Config {
    slideshows: Vec<SlideshowConfig>,
}

#[derive(Serialize, Deserialize, Debug)]
struct SlideshowConfig {
    path: PathBuf,
    width: u32,
    height: u32,
    min_aspect_ratio: f64,
    max_aspect_ratio: f64,
    min_creation_date: NaiveDate,
    max_creation_date: NaiveDate,
    image_dirs: Vec<PathBuf>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            slideshows: vec![],
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct ImageInfo {
    path: PathBuf,
    width: u32,
    height: u32,
    creation_date_time: NaiveDateTime,
}

impl ImageInfo {
    async fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        if let Some(image_info) = cached_image_info(path.as_ref()).await {
            return Ok(image_info);
        }

        let path = path.as_ref();
        // use the most old date for the creation date (exif, ctime, mtime)
        let mut date_time_candidates: Vec<NaiveDateTime> = Vec::new();
        let metadata = tokio::fs::metadata(path).await?;

        let creation_time = metadata.created()?;
        date_time_candidates.push(get_local_naive_date_time_from_system_time(creation_time)?);

        let modification_time = metadata.modified()?;
        date_time_candidates.push(get_local_naive_date_time_from_system_time(modification_time)?);

        let mut media_parser = AsyncMediaParser::new();
        let ms = AsyncMediaSource::file_path(path).await?;
        if ms.has_exif() {
            let iter: Result<ExifIter, _> = media_parser.parse(ms).await;
            match iter {
                Ok(iter) => {
                    for exif in iter {
                        let value = exif.get_value().ok_or_else(|| Error::ExifValueError(path.to_path_buf(), format!("{:?}", exif)))?;
                        let tag = match exif.tag() {
                            Some(tag) => tag,
                            None => {
                                // unknown tag
                                continue;
                            },
                        };
                        match tag {
                            ExifTag::DateTimeOriginal |
                                ExifTag::CreateDate |
                                ExifTag::ModifyDate => {
                                let date_time = value.as_time().ok_or_else(|| Error::ExifTimeError(path.to_path_buf(), tag.to_string(), value.to_string()))?;
                                let date_time = date_time.naive_local();
                                date_time_candidates.push(date_time);
                            }
                            _ => {}
                        }
                    }
                },
                Err(e) => {
                    // ignore error
                    eprintln!("Failed to parse exif, ignore exif info: {}: {:?}", path.display(), e);
                }
            }
        }

        if date_time_candidates.is_empty() {
            return Err(Error::NoCreationDateError(path.to_path_buf()).into());
        }

        let creation_date_time = date_time_candidates.iter().min().expect("checked not empty").clone();
        let (width, height) = read_image_size(path).await?;
        let result = Self {
            path: path.to_path_buf(),
            width,
            height,
            creation_date_time,
        };

        // cache the result to local
        cache_image_info(&result).await?;

        Ok(result)
    }
}

fn get_local_naive_date_time_from_system_time(system_time: SystemTime) -> Result<NaiveDateTime> {
    let system_time = system_time.duration_since(SystemTime::UNIX_EPOCH)?;
    let system_time = Local.timestamp_opt(system_time.as_secs() as i64, system_time.subsec_nanos()).earliest().ok_or_else(|| Error::SystemTimeError(system_time.as_secs().to_string()))?;
    Ok(system_time.naive_local())
}

async fn read_image_size(path: impl Into<PathBuf>) -> Result<(u32, u32)> {
    let path = path.into();
    task::spawn_blocking(move || {
        let img = image::open(path)?;
        Ok(img.dimensions())
    }).await?
}

async fn cached_image_info(path: impl AsRef<Path>) -> Option<ImageInfo> {
    let cache_path = match cache_path(path).await {
        Ok(cache_path) => cache_path,
        Err(_) => return None,
    };
    if cache_path.exists() {
        let json = match tokio::fs::read_to_string(cache_path).await {
            Ok(json) => json,
            Err(e) => {
                eprintln!("Failed to read cache file: {:?}", e);
                return None;
            }
        };
        let image_info: ImageInfo = match serde_json::from_str(&json) {
            Ok(image_info) => image_info,
            Err(e) => {
                eprintln!("Failed to parse cache file: {:?}", e);
                return None;
            }
        };
        Some(image_info)
    } else {
        None
    }
}

async fn cache_image_info(image_info: &ImageInfo) -> Result<()> {
    let cache_path = cache_path(&image_info.path).await?;
    let json = serde_json::to_string(image_info)?;
    tokio::fs::write(cache_path, json).await?;
    Ok(())
}

async fn cache_path(path: impl AsRef<Path>) -> Result<PathBuf> {
    let path = path.as_ref();
    let cache_hash = format!("{:x}", md5::compute(path.as_os_str().as_encoded_bytes()));
    let cache_parent_dir = cache_parent_dir().await?;
    Ok(cache_parent_dir.join(cache_hash + ".json"))
}

async fn cache_parent_dir() -> Result<PathBuf> {
    let cache_dir = cache_dir().ok_or(Error::CacheDirError)?;
    let cache_parent_dir = cache_dir.join(crate_name!());
    if !cache_parent_dir.exists() {
        tokio::fs::create_dir_all(&cache_parent_dir).await?;
    }
    Ok(cache_parent_dir)
}

#[derive(Debug)]
struct SlideshowWriter {
    file: tokio::fs::File,
}

impl SlideshowWriter {
    async fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file = tokio::fs::OpenOptions::new().create(true).write(true).truncate(true).open(path).await?;
        Ok(Self {
            file,
        })
    }

    async fn write_header(&mut self, width: u32, height: u32) -> Result<()> {
        let header = format!(r#"# Slide Show Sequence v2
UseTimer = 1
Timer = 2
Loop = 1
FullScreen = 0
WinWidth = {width}
WinHeight = {height}
Stretch = 1
RandomOrder = 1
ShowInfo = 1
Info = {{Filename}}
TitleBar = 1
OnTop = 1
CursorAutoHide = 0
BackgroundColor = 0 0 0 255
TextColor = 255 255 255 255
UseTextBackColor = 0
TextPosition = 0
TextBackColor = 128 128 128 255
Opacity = 100
Font = Sans Serif,9,-1,5,50,0,0,0,0,0
EffectDuration = 1000
"#, width = width, height = height);
        tokio::io::AsyncWriteExt::write_all(&mut self.file, header.as_bytes()).await?;
        Ok(())
    }

    async fn write_image_path(&mut self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        let path = path.to_string_lossy();
        let path = path.replace("\\", "\\\\").replace("\"", "\\\"");
        let line = format!("\"{}\"\n", path);
        tokio::io::AsyncWriteExt::write_all(&mut self.file, line.as_bytes()).await?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let n_threads = num_cpus::get();
    let config = jdt::project(crate_name!()).config::<Config>();
    for slideshow in config.slideshows {
        let mut slideshow_writer = SlideshowWriter::from_path(&slideshow.path).await?;
        slideshow_writer.write_header(slideshow.width, slideshow.height).await?;

        let image_path_stream = image_path_stream(slideshow.image_dirs.clone());
        let image_info_stream = image_info_stream(n_threads, image_path_stream);
        tokio::pin!(image_info_stream);
        while let Some(image_info) = image_info_stream.next().await {
            let image_info = image_info?;
            if image_info.creation_date_time.date() < slideshow.min_creation_date {
                continue;
            }
            if image_info.creation_date_time.date() > slideshow.max_creation_date {
                continue;
            }
            let aspect_ratio = image_info.width as f64 / image_info.height as f64;
            if aspect_ratio < slideshow.min_aspect_ratio || aspect_ratio > slideshow.max_aspect_ratio {
                continue;
            }
            slideshow_writer.write_image_path(&image_info.path).await?;
        }
    }
    Ok(())
}

fn image_path_stream(dirs: Vec<PathBuf>) -> impl futures::Stream<Item = Result<PathBuf>> {
    let mut dir_stack = dirs;
    stream! {
        while let Some(dir) = dir_stack.pop() {
            let mut entries = tokio::fs::read_dir(dir).await?;
            while let Some(entry) = entries.next_entry().await? {
                if junk_file::is_junk(entry.path()) {
                    continue;
                }
                if entry.file_type().await?.is_dir() {
                    dir_stack.push(entry.path());
                } else {
                    let mimes = mime_guess::from_path(entry.path());
                    let guess_image = mimes.iter().any(|mime| mime.type_() == "image");
                    if !guess_image {
                        continue;
                    }
                    yield Ok(entry.path());
                }
            }
        }
    }
}

fn image_info_stream(n_threads: usize, image_path_stream: impl futures::Stream<Item = Result<PathBuf>>) -> impl futures::Stream<Item = Result<ImageInfo>> {
    image_path_stream.map(|image_path| async {
        let image_path = image_path?;
        let image_info = ImageInfo::from_path(image_path).await?;
        Ok(image_info)
    }).buffer_unordered(n_threads)
}

