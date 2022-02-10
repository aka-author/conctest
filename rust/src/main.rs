// * * ** *** ***** ******** ************* *********************
// Observing concurrent code execution on Rust
//                                                   (\(\
//                                                  =(^.^)=
// * * ** *** ***** ******** ************* *********************

use std::env;
use std::panic;
use std::time::*;
use num_cpus;
use rand;
use thousands::Separable;
use regex::Regex;
use crossbeam::ScopedJoinHandle;
use std::path::Path;
use std::fs::File;
use std::io::Write;


// Retrieving system parameters

fn count_cpus() -> usize {
    num_cpus::get()
}


// Measuring time 

fn get_timestamp(clock: &SystemTime) -> u128 {
    match clock.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => {
            return duration.as_millis();
        }
        Err(e) => {
            panic!("Clock runtime error: {}.", e);
        }
    }
}

fn get_duration(clock: &SystemTime) -> u128 {
    match clock.elapsed() {
        Ok(elapsed) => {
            return elapsed.as_millis();
        }
        Err(e) => {
            panic!("Clock runtime error: {}.", e);
        }
    }
}

struct ScheduleEntry {
    started_at: u128,
    duration: u128
}

type Schedule = Vec<ScheduleEntry>;


// Spending time

type Triplet = (f64, f64, f64);

fn random_item() -> f64 {    
    rand::random()
}

fn random_triplet() -> Triplet {
    (random_item(), random_item(), random_item())
}

fn get_next_triplet(triplet: Triplet) -> Triplet {
    (triplet.1, triplet.2, triplet.0 + triplet.1 - triplet.2)
}

fn iterate(initial_triplet: Triplet, iterations: usize) {

    let mut triplet = initial_triplet;

    for _ in 0..iterations {
        triplet = get_next_triplet(triplet);
    }    
}

fn complex_task(iterations: usize) -> ScheduleEntry {    

    let mut entry = ScheduleEntry{started_at: 0u128, duration: 0u128};    

    let clock = SystemTime::now();
    
    entry.started_at = get_timestamp(&clock);

    iterate(random_triplet(), iterations);

    entry.duration = get_duration(&clock);

    entry
}


// Performing observations

struct ObservationOutcome {
    mean_task_duration: u128,
    absolute_concurrent_duration: u128,
    relative_concurrent_duration: f64,
    concurrency_profit: f64,
    task_schedule: Schedule
}

fn count_tasks(oo: &ObservationOutcome) -> usize {
    oo.task_schedule.len()
}

fn last_task(oo: &ObservationOutcome) -> usize {
    count_tasks(oo) - 1
}

fn finished_at(entry: &ScheduleEntry) -> u128 {
    entry.started_at + entry.duration
}

fn absolute_concurrent_duration(oo: &ObservationOutcome) -> u128 {

    let mut earliest_start = oo.task_schedule[last_task(oo)].started_at;
    let mut latest_finish= finished_at(&oo.task_schedule[last_task(oo)]); 
    let mut finish_candidate: u128;

    for entry in &oo.task_schedule {
        
        if earliest_start > entry.started_at {
            earliest_start = entry.started_at;
        }

        finish_candidate = finished_at(entry);
        if latest_finish < finish_candidate {
            latest_finish = finish_candidate;
        }
    }

    latest_finish - earliest_start
}

fn sum_duration(oo: &ObservationOutcome) -> u128 {

    let mut sum_duration: u128 = 0u128;
    
    for entry in &oo.task_schedule {
        sum_duration += entry.duration;
    }

    sum_duration    
}

fn expected_sequential_duration(oo: &ObservationOutcome) -> u128 {
    sum_duration(oo)
}

fn mean_task_duration(oo: &ObservationOutcome) -> u128 {
    sum_duration(oo)/oo.task_schedule.len() as u128        
}

fn relative_concurrent_duration(oo: &ObservationOutcome) -> f64 {
    (oo.absolute_concurrent_duration as f64)/(oo.mean_task_duration as f64)
}

fn concurrency_profit(oo: &ObservationOutcome) -> f64 {
    1.0 - oo.absolute_concurrent_duration as f64/
          expected_sequential_duration(&oo) as f64
}

fn observation_outcome(task_schedule: Schedule) -> ObservationOutcome {

    let mut oo = 
        ObservationOutcome {
            mean_task_duration: 0u128,
            absolute_concurrent_duration: 0u128,
            relative_concurrent_duration: 0f64,
            concurrency_profit: 0f64, 
            task_schedule
        };    
    
    oo.absolute_concurrent_duration = absolute_concurrent_duration(&oo);
    oo.mean_task_duration = mean_task_duration(&oo);
    oo.relative_concurrent_duration = relative_concurrent_duration(&oo);
    oo.concurrency_profit = concurrency_profit(&oo); 
        
    oo    
}

fn fulfil_observation(tasks: usize, iterations: usize) -> ObservationOutcome {

    let mut handles: Vec<ScopedJoinHandle<ScheduleEntry>> = 
                                    Vec::with_capacity(iterations-1); 

    crossbeam::scope(|spawner| {
            for _ in 0..tasks {
                handles.push(spawner.spawn(|| {complex_task(iterations)})); 
            }
        }
    );

    let mut task_schedule: Schedule = Vec::with_capacity(handles.capacity());
    for handle in handles {
        task_schedule.push(handle.join());
    }

    observation_outcome(task_schedule)
}


// Formatting and printing output data

fn print_help() {
    println!("Commands and arguments");
    println!("Displaying system parameters:");
    println!("s");
    println!("Measuring profits of concurrency:");
    println!("p <Maximal number of tasks per CPU> <Number of iterations per task> [Output file]");
    println!("Measuring delays of concurrent threads:");
    println!("d <Number of tasks per CPU> <Number of iterations per task> [Output file]");
}

fn print_report_header() {
    println!("Testing concurrent code execution on Rust");
    println!("");
}

fn print_sysparams_header() {
    println!("==========================================");
    println!("System parameter               Value");
    println!("==========================================");
}

fn print_cpus(cpus: usize) {
    println!("CPUs available {:27}", cpus);
}

fn print_iterations_per_sec(iterations_per_sec: usize) {
    println!("Iterations per second {:>20}", iterations_per_sec.separate_with_commas());
}

fn print_profit_header() {
    println!("==============================================================");
    println!("Tasks  Mean task durstion  Duration  Relative duration  Profit");
    println!("==============================================================");
}

fn format_profit_entry(oo: &ObservationOutcome) -> String {
    
    format!("{:5}, {:5}, {:9}, {:18.3}, {:6}%\n", 
            count_tasks(oo),
            oo.mean_task_duration, 
            oo.absolute_concurrent_duration, 
            oo.relative_concurrent_duration, 
            oo.concurrency_profit)
}

fn print_profit_entry(oo: &ObservationOutcome) {
    
    println!("{:5} {:19} {:9} {:18.3} {:6.0}%", 
             count_tasks(oo),
             oo.mean_task_duration, 
             oo.absolute_concurrent_duration, 
             oo.relative_concurrent_duration, 
             oo.concurrency_profit*100.0);
}

fn print_profit_separator() {
    println!("--------------------------------------------------------------");
}

fn print_report_footer() {
    println!("==============================================================");
}


// Performing observations

fn print_sysparams() {

    let cpus = count_cpus();
    
    print_sysparams_header();
    print_cpus(cpus);
    let iterations_per_sec = measure_iterations_per_sec();
    print_iterations_per_sec(iterations_per_sec);
    print_report_footer();
}

fn measure_iterations_per_sec() -> usize {

    let mut iterations_per_sec: usize = 0;

    let clock = SystemTime::now();
    let mut mills: u128 = 0;

    let mut triplet = random_triplet();

    while mills <= 1000 {

        triplet = get_next_triplet(triplet);
        
        match clock.elapsed() {
            Ok(elapsed) => {
                iterations_per_sec += 1; 
                mills = elapsed.as_millis();
            }
            Err(e) => {
                panic!("Clock runtime error: {}.", e);
            }
        }
    }

    iterations_per_sec
}

fn measure_concurrency_profit(max_tasks_per_cpu: usize, iterations: usize) -> String {
    
    let mut out_data: String = "".to_string();

    let mut tasks: usize;
    let mut oo: ObservationOutcome;
            
    print_profit_header();

    let cpus = count_cpus();

    for tasks_per_cpu in 0..max_tasks_per_cpu {
        for cpu in 0..cpus {
            tasks = 1 + cpu + tasks_per_cpu*cpus;
            oo = fulfil_observation(tasks, iterations);
            out_data += &format_profit_entry(&oo);
            print_profit_entry(&oo);
        }

        if tasks_per_cpu < max_tasks_per_cpu - 1 {
            print_profit_separator();
        }
    } 

    print_report_footer();

    out_data
}

fn measure_start_delays(tasks_per_cpu: usize, iterations: usize) -> String {
        
    let mut out_data: String = "".to_string();

    let cpus = count_cpus();

    let tasks = tasks_per_cpu*cpus;
    let oo = fulfil_observation(tasks, iterations);

    let mut delay: u128;
    let mut task_no: usize;

    for task in 0..count_tasks(&oo) {
        task_no = task + 1;
        delay = oo.task_schedule[task].started_at - &oo.task_schedule[0].started_at;
        println!("{}\t{}",task_no, delay);
        out_data += &format!("{},{}\n", task_no, delay);
    }
        
    out_data
}


// Accepting arguments

type ArgsVec = Vec<String>;

#[derive(PartialEq)]
enum Command {
    Help,
    RequestSysParams,
    MeasureConcurrencyProfit,
    MeasureStartDelays
}

const ARG_IDX_COMMAND: usize = 1;
const ARG_IDX_TASKS_PER_CPU: usize = 2;
const ARG_IDX_ITERATIONS: usize = 3;
const ARG_IDX_OUT_FILE_PATH: usize = 4;

fn validate_usize(s: &str) -> bool {   
    Regex::new(r"^\d+$").unwrap().is_match(&s)
}

fn parse_usize(s: &String) -> usize {
    s.parse::<usize>().unwrap()
}

fn validate_args(args: &ArgsVec) -> bool {
    validate_usize(&args[ARG_IDX_TASKS_PER_CPU]) && 
    validate_usize(&args[ARG_IDX_ITERATIONS])
}

fn accept_command(args: &ArgsVec) -> Command {

    let mut cmd: Command = Command::Help;

    if args.len() > 1 {
        match &*args[ARG_IDX_COMMAND] {
            "s" => {cmd = Command::RequestSysParams;}
            "p" => {cmd = Command::MeasureConcurrencyProfit;}
            "d" => {cmd = Command::MeasureStartDelays;}
            _   => {cmd = Command::Help;}
        }
    } 

    cmd
}

fn accept_tasks_per_cpu(args: &ArgsVec) -> usize {
    parse_usize(&args[ARG_IDX_TASKS_PER_CPU])
}

fn accept_iterations(args: &ArgsVec) -> usize {
    parse_usize(&args[ARG_IDX_ITERATIONS])
}

fn accept_out_file_path(args: &ArgsVec) -> &str {
    if args.len() == ARG_IDX_OUT_FILE_PATH + 1 {
        return &args[ARG_IDX_OUT_FILE_PATH];
    } else {
        return "";
    }
}


// Doing the job 

fn write_out_data(file_path: &str, out_data: &String) {

    if file_path != "" {

        let out_file_path = Path::new(file_path);

        match File::create(out_file_path) {
            Ok(mut out_file) => {
                out_file.write_all(&out_data.as_bytes()).unwrap();
            }   
            Err(e) => {
                panic!("Error while opening an output file: {}", e);
            }
        }
    } 
}

fn main() {

    print_report_header();

    let args: Vec<String> = env::args().collect();

    match accept_command(&args) {
        Command::Help => {
            print_help();
        }
        Command::RequestSysParams => {
            print_sysparams();
        }
        Command::MeasureConcurrencyProfit => {
            if validate_args(&args) {
                let out_data = measure_concurrency_profit(
                    accept_tasks_per_cpu(&args),
                    accept_iterations(&args));
                write_out_data(accept_out_file_path(&args), &out_data);
            } else {
                print_help();
            }
        }
        Command::MeasureStartDelays => {
            if validate_args(&args) {
                let out_data = measure_start_delays(
                    accept_tasks_per_cpu(&args),
                    accept_iterations(&args));
                write_out_data(accept_out_file_path(&args), &out_data);
            } else {
                print_help();
            }
        }
    }
}