use std::fs::File;
use std::io::{self, stdin, BufRead, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use clap::Parser;
use crossbeam_channel::{bounded, Sender};
use flate2::bufread::GzDecoder;
use rayon::prelude::*;
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

const DECOMPRESS_BUFFER_SIZE: usize = 2 * 1024 * 1024;
const CHANNEL_FLUSH_THRESHOLD_BYTES: usize = 16 * 1024 * 1024;
const STDOUT_BUFFER_BYTES: usize = 16 * 1024 * 1024;

#[derive(Parser, Debug)]
#[command(name = "metrics_collector")]
#[command(about = "Scan gzipped CSV files and print lines containing a given substring")]
struct CliArgs {
    #[arg(short = 'r', long = "record-substring", default_value = "")]
    record_substring: String,
}

fn filter_single_gziped_csv(
    gzip_file_path: &Path,
    record_substring: &str,
    writer_sender: &Sender<String>,
) {
    let compressed_reader = io::BufReader::with_capacity(DECOMPRESS_BUFFER_SIZE, File::open(gzip_file_path).unwrap());
    let mut gzip_decoder = GzDecoder::new(compressed_reader);

    let decompressed = {
        let mut buf = String::new();
        gzip_decoder.read_to_string(&mut buf).unwrap();
        buf
    };

    let mut output_chunk_buffer = String::with_capacity(CHANNEL_FLUSH_THRESHOLD_BYTES.saturating_mul(2));

    decompressed.lines().for_each(|line| {

        let line_matches =
        record_substring.is_empty() || line.contains(record_substring);

        if line_matches {
            output_chunk_buffer.push_str(&line);
            if output_chunk_buffer.len() >= CHANNEL_FLUSH_THRESHOLD_BYTES {
                // 所有権を移して送信（アロケーション再利用のため take）
                let _ = writer_sender.send(std::mem::take(&mut output_chunk_buffer));
            }
        }

    });

    if !output_chunk_buffer.is_empty() {
        let _ = writer_sender.send(output_chunk_buffer);
    }
}

fn main(){
    let cli_args = CliArgs::parse();

    // 標準出力の集約（ロック競合を避けたい）
    let (writer_sender, writer_receiver) = bounded::<String>(256);
    let writer_thread_handle = std::thread::spawn(move || {
        let stdout_handle = io::stdout();
        let mut stdout_writer = BufWriter::with_capacity(STDOUT_BUFFER_BYTES, stdout_handle.lock());
        while let Ok(output_chunk) = writer_receiver.recv() {
            let _ = stdout_writer.write_all(output_chunk.as_bytes());
        }
        let _ = stdout_writer.flush();
    });



    let gzip_file_paths =
            stdin()
            .lock()
            .lines()
            .filter_map(Result::ok)
            .map(|path_str| PathBuf::from(path_str.trim()))
            .collect::<Vec<_>>();

    gzip_file_paths.par_iter().for_each(|gzip_file_path| {
        let _ = filter_single_gziped_csv(
            gzip_file_path,
            &cli_args.record_substring,
            &writer_sender,
        );
    });

    // 出力スレッドを終了
    drop(writer_sender);
    let _ = writer_thread_handle.join();
}
