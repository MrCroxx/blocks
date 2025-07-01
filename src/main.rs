use chrono::{DateTime, Local, TimeZone};
use clap::Parser;
use csv::ReaderBuilder;
use regex::Regex;
use std::collections::HashMap;
use std::fs;
use std::io::{BufReader, BufWriter, Write};
use std::time::SystemTime;
use std::time::{Duration, UNIX_EPOCH};

#[derive(Debug, Parser)]
struct Args {
    dir: String,
    #[clap(short, long, default_value = "out.txt")]
    out: String,
    #[clap(short, long, default_value = "duration.txt")]
    duration: String,
}

#[derive(Debug, Clone, Copy)]
struct Data {
    sst: u64,
    blk: u64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Op {
    Evicted,
    Missed,
}

fn parse(s: &str) -> Vec<(Data, SystemTime, Op)> {
    let op = if s.contains("========== EVICTED DATA BLOCKS ==========") {
        Op::Evicted
    } else if s.contains("========== MISSED DATA BLOCKS ==========") {
        Op::Missed
    } else {
        return vec![];
    };

    // 定义正则表达式，匹配 sst_id 和 block_idx 以及时间戳
    let re = Regex::new(r"SstableBlockIndex \{ sst_id: (\d+), block_idx: (\d+) \}, SystemTime \{ tv_sec: (\d+), tv_nsec: (\d+) \}").unwrap();

    let mut res = vec![];

    for cap in re.captures_iter(s) {
        let sst = cap[1].parse::<u64>().unwrap();
        let blk = cap[2].parse::<u64>().unwrap();
        let tv_sec = cap[3].parse::<u64>().unwrap();
        let tv_nsec = cap[4].parse::<u32>().unwrap();

        let data = Data { sst, blk };
        let system_time = UNIX_EPOCH + Duration::new(tv_sec, tv_nsec);

        res.push((data, system_time, op));
    }

    res
}

fn main() {
    let args = Args::parse();

    let mut records = vec![];
    let mut evicted_times: HashMap<(u64, u64), SystemTime> = HashMap::new();

    for entry in fs::read_dir(&args.dir).unwrap() {
        let entry = entry.unwrap();
        let file_path = entry.path();

        if file_path.extension().and_then(|ext| ext.to_str()) == Some("csv") {
            let file = fs::File::open(&file_path).unwrap();
            let buffered_reader = BufReader::new(file);
            let mut reader = ReaderBuilder::new()
                .has_headers(true)
                .from_reader(buffered_reader);

            for result in reader.records() {
                let record = result.unwrap();
                let rs = parse(record.as_slice());
                records.extend(rs);
                let row = records.len();
                if records.len() % 10000 == 0 {
                    println!("Processed {row} records");
                }
            }
        }
    }

    println!("Sorting...");
    records.sort_by_key(|(_, time, _)| std::cmp::Reverse(*time));

    let output_file = fs::File::create(&args.out).unwrap();
    let mut writer = BufWriter::with_capacity(64 * 1024, output_file); // Use a larger buffer size for better performance

    for (row, record) in records.iter().enumerate() {
        let (data, system_time, op) = record;
        let datetime: DateTime<Local> = Local
            .timestamp_opt(
                system_time.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
                system_time
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .subsec_nanos(),
            )
            .unwrap();
        writeln!(
            writer,
            "{:?}, {}, {:?}",
            data,
            datetime.format("%Y-%m-%d %H:%M:%S%.f"),
            op
        )
        .unwrap();
        if row % 10000 == 0 {
            println!("Written {row} records");
        }

        // Store evicted times
        if *op == Op::Evicted {
            evicted_times.insert((data.sst, data.blk), *system_time);
        }
    }

    // Calculate durations between evicted and missed events
    let duration_file = fs::File::create(&args.duration).unwrap();
    let mut duration_writer = BufWriter::new(duration_file);

    let mut long = 0;
    let mut short = 0;
    let mut none = 0;

    for (data, system_time, op) in &records {
        if *op == Op::Missed {
            let datetime: DateTime<Local> = Local
                .timestamp_opt(
                    system_time.duration_since(UNIX_EPOCH).unwrap().as_secs() as i64,
                    system_time
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .subsec_nanos(),
                )
                .unwrap();
            let miss = datetime.format("%Y-%m-%d %H:%M:%S%.f");
            if let Some(&evicted_time) = evicted_times.get(&(data.sst, data.blk)) {
                if evicted_time > *system_time {
                    let duration = evicted_time.duration_since(*system_time).unwrap();
                    writeln!(
                        duration_writer,
                        "{data:?}, delta: -{duration:?}, miss time: {miss}"
                    )
                    .unwrap();
                } else {
                    let duration = system_time.duration_since(evicted_time).unwrap();

                    let suffix = if duration.as_secs_f64() < 10.0 {
                        short += 1;
                        "!!!!!!!!!!"
                    } else {
                        long += 1;
                        ""
                    };
                    writeln!(
                        duration_writer,
                        "{data:?}, delta: {duration:?}, miss time: {miss} {suffix}"
                    )
                    .unwrap();
                }
            } else {
                none += 1;
                writeln!(
                    duration_writer,
                    "{:?}, miss time: {miss}, No evicted time found",
                    data
                )
                .unwrap();
            }
        }
    }

    writeln!(
        duration_writer,
        "long: {long}, short: {short}, none: {none}"
    )
    .unwrap();

    println!("Done. Total records: {}", records.len());
}
