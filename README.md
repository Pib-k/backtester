# Tick Data Backtester

_The Current Goal_: Parse and backtest 10GB of historical market data in under 10 seconds.

## Learning Steps

### Phase 1: Text Parsing  

* Approach: Read raw CSV strings and parse them into `f64` floats line-by-line using `serde` and standard standard File I/O.  
* Bottleneck: UTF-8 string allocation and float parsing.  
* Benchmark (10M rows): 2.5 seconds.

### Phase 2: Binary Formats  

* Approach: Eliminated text parsing overhead by pre-converting the dataset to MessagePack (`rmp-serde`) and deserializing raw bytes.  
* Bottleneck: Standard standard I/O streams and heap allocations for the `String` ticker field.  
* Benchmark (10M rows): 1.75s

### Phase 3: Memory Mapping  

* Approach: Bypass standard OS file reading by mapping the binary file directly into virtual memory using `memmap2`.  
* Benchmark (10M rows): TBD

## Hardware   * CPU: AMD Ryzen 7 7800X3D (8-Core, 16-Thread)  

* RAM: 32GB DDR5  
* Storage: Samsung SSD 970 EVO PLUS 2TB, (PCIe Gen 3.0 x4 NVMe via MAG B650 TOMAHAWK WIFI)  
* OS: Windows 11 Pro (Build 22631)  
* Rust Version: rustc 1.80.0 (or latest stable)
