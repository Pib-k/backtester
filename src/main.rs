use csv::Reader;
use csv::Writer;
use rand::RngExt;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::fs::File;
use std::path::Path;
use std::time::Instant;
use std::time::*;

#[derive(Serialize, Debug)]
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
    let file_path = "output.csv";
    let num_rows = 10_000_000;

    if !Path::new(file_path).exists() {
        create_csv(num_rows, file_path);
    }

    let mut reader = Reader::from_path(&file_path).unwrap();
    let window_size = 50;
    let mut market_state: HashMap<String, TickerState> = HashMap::new();

    let start_time = Instant::now();

    for result in reader.records() {
        let record = match result {
            Ok(r) => r,
            Err(e) => {
                eprintln!("{}", e);
                continue;
            }
        };

        let ticker = record[1].to_string();
        let price = record[2].parse::<f64>().unwrap_or(0.0);

        let state = market_state.entry(ticker).or_insert(TickerState {
            window: VecDeque::with_capacity(window_size + 1),
            sum_prices: 0.0,
        });

        state.sum_prices += price;
        state.window.push_back(price);

        if state.window.len() > window_size {
            if let Some(removed_price) = state.window.pop_front() {
                state.sum_prices -= removed_price;
            }
        }
    }

    let duration = start_time.elapsed();
    println!("Processed {} rows in: {:?}", num_rows, duration);

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
