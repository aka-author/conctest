// * * ** *** ***** ******** ************* *********************
// Observing concurrent code execution on Rust
//                                                   (\(\
//                                                  =('.')=
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

    for _i in 0..iterations {
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

struct Observation {
    total_duration: u128,
    schedule: Schedule
}

fn fulfil_observation(tasks: usize, iterations: usize) -> Observation {

    let mut observation = 
        Observation {
            total_duration: 0u128, 
            schedule: Vec::with_capacity(iterations-1)
        };        

    let clock = SystemTime::now();

    let mut handles: Vec<ScopedJoinHandle<ScheduleEntry>>; 

    handles = Vec::with_capacity(iterations-1);

    crossbeam::scope(|spawner| {
            for _task_idx in 0..tasks {
                handles.push(spawner.spawn(|| {complex_task(iterations)})); 
            }
        }
    );

    for handle in handles {
        observation.schedule.push(handle.join());
    }

    observation.total_duration = get_duration(&clock);

    observation
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

fn measure_task_duration(iterations: usize) -> u128 {

    let observations = 10;

    let mut total_duration: u128 = 0;

    for _i in 0..observations {
        total_duration += fulfil_observation(1, iterations).total_duration;
    }

    total_duration/observations
}


// Formatting and printing output data

fn print_report_header() {
    println!("Testing concurent code execution in Rust");
    println!("");
}

fn print_report_sysparams_header() {
    println!("==========================================");
    println!("System parameter               Value");
    println!("==========================================");
}

fn print_report_table_header() {
    println!("==========================================");
    println!("Tasks  Duration  Relative duration  Profit");
    println!("==========================================");
}

fn report_cpus(cpus: usize) {
    println!("CPUs available {:27}", cpus);
}

fn report_iterations_per_sec(iterations_per_sec: usize) {
    println!("Iterations per second {:>20}", iterations_per_sec.separate_with_commas());
}
 
fn report_table_entry(tasks: usize, task_duration: u128, concurrent_duration: u128) -> String {
    
    let k = concurrent_duration as f32/task_duration as f32;
    let total_duration = tasks as u128 * task_duration;
    let profit = 100*(total_duration as i128 - concurrent_duration as i128)/total_duration as i128;
    
    println!("{:5} {:9} {:18.3} {:6}%", tasks, concurrent_duration, k, profit);

    format!("{:5}, {:9}, {:18.3}, {:6}%\n", tasks, concurrent_duration, k, profit)
}

fn print_report_table_separator() {
    println!("------------------------------------------");
}

fn print_report_table_footer() {
    println!("==========================================");
}

fn print_help() {
    println!("Commands and arguments");
    println!("Displaying system parameters:");
    println!("s");
    println!("Measuring profits of concurrency:");
    println!("p <Maximal number of tasks per CPU> <Number of iterations per task> [Output file]");
    println!("Measuring delays of concurrent threads:");
    println!("d <Number of tasks per CPU> <Number of iterations per task> [Output file]");
}


// Performing observations

fn print_sysparams() {

    let cpus = count_cpus();
    
    print_report_sysparams_header();
    report_cpus(cpus);
    let iterations_per_sec = measure_iterations_per_sec();
    report_iterations_per_sec(iterations_per_sec);
    print_report_table_footer();
}

fn measure_concurrent_profit(max_tasks_per_cpu: usize, iterations: usize) -> String {
    
    let mut out_data: String = "".to_string();

    let mut tasks: usize;
    let mut observation: Observation;
            
    print_report_table_header();

    let cpus = count_cpus();

    let task_duration = measure_task_duration(iterations);

    for tasks_per_cpu in 0..max_tasks_per_cpu {
        for cpu in 0..cpus {
            tasks = 1 + cpu + tasks_per_cpu*cpus;
            observation = fulfil_observation(tasks, iterations);
            out_data += &report_table_entry(tasks, task_duration, observation.total_duration);
        }

        if tasks_per_cpu < max_tasks_per_cpu - 1 {
            print_report_table_separator();
        }
    } 

    print_report_table_footer();

    out_data
}

fn measure_start_delays(tasks_per_cpu: usize, iterations: usize) -> String {
        
    let mut out_data: String = "".to_string();

    let cpus = count_cpus();

    let tasks = tasks_per_cpu*cpus;
    let observation = fulfil_observation(tasks, iterations);

    let mut delay: u128;
    let mut task_no: usize;

    for task in 0..observation.schedule.len() {
        task_no = task + 1;
        delay = observation.schedule[task].started_at - &observation.schedule[0].started_at;
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
    MeasureConcurentProfit,
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
            "p" => {cmd = Command::MeasureConcurentProfit;}
            "d" => {cmd = Command::MeasureStartDelays;}
            _   => {cmd = Command::Help;}
        }
    } 

    cmd
}

fn accept_tasks_per_cpu(args: &ArgsVec) -> usize {
    println!("ARG_IDX_TASKS_PER_CPU {}", parse_usize(&args[ARG_IDX_TASKS_PER_CPU]));
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
        Command::MeasureConcurentProfit => {
            if validate_args(&args) {
                let out_data = measure_concurrent_profit(
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