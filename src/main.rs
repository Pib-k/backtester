use csv::Reader;
use csv::Writer;
use rand::RngExt;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fs::File;
use std::io::*;
use std::path::Path;
use std::time::*;

#[derive(Serialize, Debug, Deserialize)]
struct Tick {
    timestamp: u64,
    ticker: String,
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
    let mmap = unsafe { memmap::MmapOptions::new().map(&file).unwrap() };

    let mut reader = Cursor::new(&mmap);
    let mut rows_processed = 0;

    let window_size = 50;
    let mut market_state: HashMap<String, TickerState> = HashMap::new();
    let start_time = Instant::now();

    while let Ok(tick) = rmp_serde::decode::from_read::<_, Tick>(&mut reader) {
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

    for (ticker, state) in market_state.iter() {
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
    let mut rng = rand::rng();
    let stocks = ["TSLA", "NVDIA", "APL", "AMZN", "GOOG"];

    let mut current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    for _n in 0..num_rows {
        let tick = Tick {
            timestamp: current_time,
            ticker: stocks[rng.random_range(0..stocks.len())].to_string(),
            price: rng.random_range(100.0..500.0),
            volume: rng.random_range(1.0..100.0),
        };
        writer.serialize(&tick).unwrap();
        current_time += rng.random_range(0..3);
    }

    writer.flush().unwrap();

    println!("FINISHED CREATING CSV FILE");
}

fn convert_csv_to_bin(csv_path: &str, bin_path: &str) {
    let mut csv_reader = Reader::from_path(csv_path).unwrap();

    let bin_file = File::create(bin_path).unwrap();
    let mut bin_writer = BufWriter::new(bin_file);

    for result in csv_reader.records() {
        if let Ok(record) = result {
            let tick = Tick {
                timestamp: record[0].parse().unwrap_or(0),
                ticker: record[1].to_string(),
                price: record[2].parse().unwrap_or(0.0),
                volume: record[3].parse().unwrap_or(0.0),
            };
            rmp_serde::encode::write(&mut bin_writer, &tick).unwrap();
        }
    }
    bin_writer.flush().unwrap();
    println!("CONVERSION COMPLETE");
}
