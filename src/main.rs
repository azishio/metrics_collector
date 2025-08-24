use std::fs::File;
use std::io::{self, stdin, BufRead, BufReader, BufWriter, Read, Write};
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

// でか過ぎるバッファはメモリ圧を生むので控えめに
const DECOMPRESS_BUFFER_SIZE: usize = 256 * 1024;      // 256 KiB
const CHANNEL_FLUSH_THRESHOLD_BYTES: usize = 1 * 1024 * 1024; // 1 MiB
const STDOUT_BUFFER_BYTES: usize = 1 * 1024 * 1024;    // 1 MiB

#[derive(Parser, Debug)]
#[command(name = "metrics_collector")]
#[command(about = "Scan gzipped CSV files and print lines containing a given substring")]
struct CliArgs {
    #[arg(short = 'r', long = "record-substring", default_value = "")]
    record_substring: String,

    #[arg(short = 'c', long = "channel-bound", default_value = "1000")]
    channel_bound: usize,
}

fn filter_single_gziped_csv(
    gzip_file_path: &Path,
    record_substring: &str,
    writer_sender: &Sender<String>,
) -> io::Result<()> {
    // 入力は二段Buf：File → BufReader → GzDecoder(読み) → BufReader(行読み)
    let file = File::open(gzip_file_path)?;
    let compressed_reader = BufReader::with_capacity(DECOMPRESS_BUFFER_SIZE, file);
    let gzip_decoder = GzDecoder::new(compressed_reader);
    let mut dec = BufReader::with_capacity(DECOMPRESS_BUFFER_SIZE, gzip_decoder);

    let mut line = String::new();
    let mut out = String::with_capacity(CHANNEL_FLUSH_THRESHOLD_BYTES * 2);

    loop {
        line.clear();
        let n = dec.read_line(&mut line)?;
        if n == 0 { break; }

        let matched = record_substring.is_empty() || line.contains(record_substring);

        if matched {
            out.push_str(&line);
            if out.len() >= CHANNEL_FLUSH_THRESHOLD_BYTES {
                let _ = writer_sender.send(std::mem::take(&mut out));
            }
        }
    }

    if !out.is_empty() {
        let _ = writer_sender.send(out);
    }
    Ok(())
}

fn main() {
    let cli_args = CliArgs::parse();

    // 標準出力の集約スレッド（小さめバッファ & 逐次flush）
    let (writer_sender, writer_receiver) = bounded::<String>(cli_args.channel_bound);
    let writer_handle = std::thread::spawn(move || {
        let stdout_handle = io::stdout();
        let mut stdout_writer = BufWriter::with_capacity(STDOUT_BUFFER_BYTES, stdout_handle.lock());
        while let Ok(chunk) = writer_receiver.recv() {
            let _ = stdout_writer.write_all(chunk.as_bytes());
        }
        let _ = stdout_writer.flush();
    });

    // 入力: 標準入力のパス列を逐次→並列に橋渡し（全件Vecに貯めない）
    stdin()
        .lock()
        .lines()
        .filter_map(Result::ok)
        .map(|s| PathBuf::from(s.trim()))
        .collect::<Vec<_>>()
        .par_iter()
        .for_each(|gzip_file_path| {
            let _ = filter_single_gziped_csv(
                &gzip_file_path,
                &cli_args.record_substring,
                &writer_sender,
            );
        });

    drop(writer_sender);
    let _ = writer_handle.join();
}
