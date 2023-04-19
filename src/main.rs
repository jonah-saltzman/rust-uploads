use std::collections::HashMap;
use std::sync::{Arc};
use warp::body::BodyDeserializeError;
use warp::reject::Reject;
use warp::{Filter, Rejection, Reply, reply, http::StatusCode};
use uuid::Uuid;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, AsyncSeekExt};
use tokio::sync::Mutex;
use serde::{Serialize, Deserialize};

type Uploads = Arc<Mutex<HashMap<Uuid, File>>>;

#[derive(Debug)]
struct BadRequest {
    message: String
}
#[derive(Debug)]
struct InternalErr {
    message: String
}

impl Reject for BadRequest {}
impl Reject for InternalErr {}

#[derive(Debug, Deserialize, Serialize)]
#[allow(non_snake_case)]
struct CreateUploadReq {
    fileSize: u64,
    fileName: String
}

#[derive(Debug, Deserialize, Serialize)]
#[allow(non_snake_case)]
struct CreateUploadRes {
    id: String
}

#[tokio::main]
async fn main() {
    let uploads: Uploads = Arc::new(Mutex::new(HashMap::new()));
    if let Err(e) = run_server(uploads, 3030).await {
        eprintln!("Server error: {}", e);
    }
}

async fn run_server(uploads: Uploads, port: u16) -> tokio::io::Result<()> {

    let uploads = warp::any().map(move || uploads.clone());

    let initiate_upload = warp::path!("upload")
        .and(warp::post())
        .and(warp::body::json())
        .and(uploads.clone())
        .and_then(initiate_upload_handler);

    let upload_chunk = warp::path!("upload" / Uuid)
        .and(warp::put())
        .and(warp::header("Content-Range"))
        .and(warp::header("Content-Id"))
        .and(warp::body::bytes())
        .and(uploads.clone())
        .and_then(upload_chunk_handler);

    let routes = initiate_upload.or(upload_chunk).recover(handle_rejection);
    println!("listening");
    warp::serve(routes).run(([127, 0, 0, 1], port)).await;
    Ok(())
}

async fn initiate_upload_handler(
    upload_request: CreateUploadReq,
    uploads: Uploads,
) -> Result<impl Reply, Rejection> {

    let id = Uuid::new_v4();
    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(format!("/tmp/{}.tmp", id))
        .await
        .map_err(|e| warp::reject::custom(InternalErr{message: e.to_string()}))?;

    let file = file.into_std().await;
    file.set_len(upload_request.fileSize).map_err(|e| warp::reject::custom(InternalErr{message: e.to_string()}))?;
    let file = File::from_std(file);

    uploads.lock().await.insert(id, file);

    Ok(warp::reply::json(&CreateUploadRes{id: id.to_string()}))
}

async fn upload_chunk_handler(
    id: Uuid,
    content_range: String,
    content_id: String,
    chunk: bytes::Bytes,
    uploads: Uploads,
) -> Result<impl Reply, Rejection> {
    if content_id != format!("{}", id) {
        return Err(warp::reject::custom(BadRequest{message: "Content-Id doesn't match".to_string()}));
    }

    let (start, _) = parse_content_range(&content_range)
        .ok_or_else(|| warp::reject::custom(BadRequest{message: "Invalid Content-Range header".to_string()}))?;

    let mut uploads = uploads.lock().await;
    let file = uploads
        .get_mut(&id)
        .ok_or_else(|| warp::reject::not_found())?;

    file.seek(std::io::SeekFrom::Start(start)).await.map_err(|e| warp::reject::custom(InternalErr{message: e.to_string()}))?;
    file.write_all(&chunk).await.map_err(|e| warp::reject::custom(InternalErr{message: e.to_string()}))?;
    file.flush().await.map_err(|e| warp::reject::custom(InternalErr{message: e.to_string()}))?;

    Ok(warp::reply::with_status("Chunk uploaded successfully", warp::http::StatusCode::OK))
}

async fn handle_rejection(err: Rejection) -> Result<impl Reply, std::convert::Infallible> {
    if err.is_not_found() {
        Ok(reply::with_status("NOT_FOUND", StatusCode::NOT_FOUND))
    } else if let Some(e) = err.find::<BadRequest>() {
        eprintln!("bad request: {}", e.message);
        Ok(reply::with_status("BAD_REQUEST", StatusCode::BAD_REQUEST))
    } else if let Some(e) = err.find::<InternalErr>() {
        eprintln!("server err: {}", e.message);
        Ok(reply::with_status("INTERNAL_SERVER_ERROR", StatusCode::INTERNAL_SERVER_ERROR))
    } else if let Some(_) = err.find::<BodyDeserializeError>() {
        Ok(reply::with_status("BAD_REQUEST", StatusCode::BAD_REQUEST))
    } else {
        eprintln!("unhandled rejection: {:?}", err);
        Ok(reply::with_status("INTERNAL_SERVER_ERROR", StatusCode::INTERNAL_SERVER_ERROR))
    }
}

fn parse_content_range(content_range: &str) -> Option<(u64, u64)> {
    let parts: Vec<&str> = content_range.split('/').collect();
    if parts.len() != 2 {
        return None;
    }

    let range_parts: Vec<&str> = parts[0].split('-').collect();
    if range_parts.len() != 2 {
        return None;
    }

    let start = range_parts[0].parse::<u64>().ok()?;
    let end = range_parts[1].parse::<u64>().ok()?;

    Some((start, end))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use fastrand::Rng;
    use reqwest::Client;
    use std::thread;
    use tokio::{task, fs::read};

    const FILE_SIZE: usize = 10 * 1024 * 1024; // 100mb
    const CHUNK_SIZE: usize = 1 * 1024 * 1024; // 10mb

    const NUM_TASKS: i32 = 50;

    #[tokio::test]
    async fn test_upload_single_large_file_and_verify() {
        
        const PORT: u16 = 3030;
        let uploads = Arc::new(Mutex::new(HashMap::new()));
        let server_uploads = uploads.clone();
        let _ = thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                run_server(server_uploads, 3030).await.unwrap();
            });
        });

        thread::sleep(Duration::from_secs(1));

        let rng = Rng::new();

        let client = Client::new();
        let random_data = generate_random_data(FILE_SIZE, rng);

        let id = initiate_upload(PORT, &client, FILE_SIZE as u64, "test_file.txt".to_string()).await.unwrap();
        upload_file_in_chunks(PORT, &client, &id, &random_data, FILE_SIZE as u64, CHUNK_SIZE, 1).await.unwrap();

        // Read the uploaded file from the server
        let uploaded_file_path = format!("/tmp/{}.tmp", id);
        let uploaded_data = read(uploaded_file_path).await.unwrap();

        // Compare the original data with the uploaded data
        assert_eq!(random_data, uploaded_data);
    }

    #[tokio::test]
    async fn test_upload_large_files() {
        const PORT: u16 = 3031;
        let uploads: Uploads = Arc::new(Mutex::new(HashMap::new()));
        let upload_clone = uploads.clone();
        let _ = thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                run_server(upload_clone, PORT).await.unwrap();
            });
        });

        thread::sleep(Duration::from_secs(1));


        // create 10 tasks to upload files concurrently
        let tasks: Vec<_> = (0..NUM_TASKS)
            .map(|i| {
                task::spawn(async move {
                    println!("task {} starting", i);
                    let rng = Rng::new();
                    let client = Client::new();
                    let random_data = generate_random_data(FILE_SIZE, rng);
                    println!("task {} generated data", i);
                    let id = initiate_upload(PORT, &client, FILE_SIZE as u64, format!("test-{}.txt", i)).await.unwrap();
                    println!("task {} initiated upload", i);
                    upload_file_in_chunks(PORT, &client, &id, &random_data, FILE_SIZE as u64, CHUNK_SIZE, i).await.unwrap();
                    println!("task {} completed upload", i);
                    let uploaded_file_path = format!("/tmp/{}.tmp", id);
                    let uploaded_data = read(uploaded_file_path).await.unwrap();
                    assert_eq!(random_data, uploaded_data);
                })
            })
            .collect();

        // Wait for all tasks to finish
        for t in tasks {
            t.await.unwrap();
        }
    }

    async fn initiate_upload(port: u16, client: &Client, file_size: u64, file_name: String) -> Result<Uuid, reqwest::Error> {
        let body = CreateUploadReq{fileName: file_name, fileSize: file_size};
        let response = client
            .post(format!("http://localhost:{}/upload", port))
            .json(&body)
            .send()
            .await?;
        let res: CreateUploadRes = response.json().await?;
        Ok(Uuid::parse_str(res.id.as_str()).unwrap())
    }

    async fn upload_file_in_chunks(
        port: u16,
        client: &Client,
        id: &Uuid,
        data: &[u8],
        file_size: u64,
        chunk_size: usize,
        task: i32,
    ) -> Result<(), reqwest::Error> {
        for (i, chunk) in data.chunks(chunk_size).enumerate() {
            println!("task {} uploading chunk {}", task, i);
            let start = i * chunk_size;
            let end = start + chunk.len() - 1;

            client
                .put(&format!("http://localhost:{}/upload/{}", port, id))
                .header("Content-Range", format!("{}-{}/{}", start, end, file_size))
                .header("Content-Id", id.to_string())
                .body(chunk.to_vec())
                .send()
                .await?;
        }
        Ok(())
    }

    fn generate_random_data(size: usize, rng: Rng) -> Vec<u8> {
        std::iter::repeat_with(|| rng.u8(..)).take(size).collect()
    }
}