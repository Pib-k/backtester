use bytemuck::{Pod, Zeroable};
use csv::{Reader, Writer};
use rand::RngExt;
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::Instant;
use std::time::*;

#[derive(Copy, Clone, Debug, Pod, Zeroable)]
#[repr(C)]
struct Tick {
    timestamp: u64,
    ticker: [u8; 8],
    price: f64,
    volume: f64,
}

struct TickerState {
    window: VecDeque<f64>,
    sum_prices: f64,
}

fn main() {
    let file_path = "output/output.csv";
    let bin_path = "output/output.bin";
    let num_rows = 10_000_000;

    if !Path::new(file_path).exists() {
        create_csv(num_rows, file_path);
    }

    if !Path::new(bin_path).exists() {
        convert_csv_to_bin(file_path, bin_path);
    }

    let file = File::open(bin_path).unwrap();
    let mmap = unsafe { memmap2::MmapOptions::new().map(&file).unwrap() };

    let window_size = 50;
    let mut market_state: HashMap<[u8; 8], TickerState> = HashMap::new();
    let start_time = Instant::now();
    let ticks: &[Tick] = bytemuck::cast_slice(&mmap);
    let mut rows_processed = 0;

    for tick in ticks {
        let state = market_state.entry(tick.ticker).or_insert(TickerState {
            window: VecDeque::with_capacity(window_size + 1),
            sum_prices: 0.0,
        });

        state.sum_prices += tick.price;
        state.window.push_back(tick.price);

        if state.window.len() > window_size {
            if let Some(removed_price) = state.window.pop_front() {
                state.sum_prices -= removed_price;
            }
        }
        rows_processed += 1;
    }

    let duration = start_time.elapsed();
    println!("Processed {} rows in: {:.4?}", rows_processed, duration);

    for (ticker_bytes, state) in market_state.iter() {
        let ticker = std::str::from_utf8(ticker_bytes)
            .unwrap()
            .trim_matches(char::from(0));

        let final_ma = state.sum_prices / state.window.len() as f64;
        println!("Final Moving Average for {}: {:.2}", ticker, final_ma);
    }
}

fn create_csv(num_rows: i32, file_path: &str) {
    println!(
        "CREATING CSV FILE WITH {} ROWS OF STOCK DATA TO {}",
        num_rows, file_path
    );
    let file = match File::create(file_path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Could not create the CSV file. Reason: {}", e);
            std::process::exit(1);
        }
    };

    let mut writer = Writer::from_writer(file);
    writer
        .write_record(&["timestamp", "ticker", "price", "volume"])
        .unwrap();

    let mut rng = rand::rng();
    let stocks = ["TSLA", "NVDIA", "APL", "AMZN", "GOOG"];

    let mut current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    for _n in 0..num_rows {
        let ticker_str = stocks[rng.random_range(0..stocks.len())];
        let price = rng.random_range(100.0..500.0);
        let volume = rng.random_range(1.0..100.0);

        writer
            .write_record(&[
                current_time.to_string(),
                ticker_str.to_string(),
                price.to_string(),
                volume.to_string(),
            ])
            .unwrap();

        current_time += rng.random_range(0..3);
    }

    writer.flush().unwrap();
    println!("FINISHED CREATING CSV FILE");
}

fn string_to_ticker(s: &str) -> [u8; 8] {
    let mut ticker = [0u8; 8];
    let bytes = s.as_bytes();
    let len = bytes.len().min(8);
    ticker[..len].copy_from_slice(&bytes[..len]);
    ticker
}

fn convert_csv_to_bin(csv_path: &str, bin_path: &str) {
    let mut csv_reader = Reader::from_path(csv_path).unwrap();

    let bin_file = File::create(bin_path).unwrap();
    let mut bin_writer = BufWriter::new(bin_file);

    for result in csv_reader.records() {
        if let Ok(record) = result {
            let tick = Tick {
                timestamp: record[0].parse().unwrap_or(0),
                ticker: string_to_ticker(&record[1]),
                price: record[2].parse().unwrap_or(0.0),
                volume: record[3].parse().unwrap_or(0.0),
            };
            bin_writer.write_all(bytemuck::bytes_of(&tick)).unwrap();
        }
    }
    bin_writer.flush().unwrap();
    println!("CONVERSION COMPLETE");
}
