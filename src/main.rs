use csv::Writer;
use rand::RngExt;
use serde::Serialize;
use std::collections::VecDeque;
use std::fs::File;
use std::io::*;
use std::os::*;
use std::time::*;

#[derive(Serialize)]
struct Tick {
    timestamp: u64,
    stock_abr: String,
    price: f64,
    volume: f64,
}

fn main() {
    let file_path = "output.csv";
    let num_rows = 10_000_000;

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
    let stocks = ["TSLA, NVDIA, APL, AMZN, GOOG"];

    let mut current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    for _n in 0..num_rows {
        let tick = Tick {
            timestamp: current_time,
            stock_abr: stocks[rng.random_range(0..stocks.len())].to_string(),
            price: rng.random_range(100.0..500.0),
            volume: rng.random_range(1.0..100.0),
        };
        writer.serialize(&tick).unwrap();
        current_time += rng.random_range(0..3);
    }

    writer.flush().unwrap();

    println!("FINISHED CREATING CSV FILE");

    let mut f = File::open(&file_path).unwrap();
    let window_size = 50;
    let mut window: VecDeque<f64> = VecDeque::new();
}
