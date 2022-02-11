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

type TimeMs = i128;
type DurationMs = i128;
type TimeCompatibleInt = i128;

fn now_ms(watch: &SystemTime) -> TimeMs {
    match watch.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => {
            return duration.as_millis() as TimeMs;
        }
        Err(e) => {
            panic!("Someone stole my watch: {}!", e);
        }
    }
}

fn duration_ms(clock: &SystemTime) -> DurationMs {
    match clock.elapsed() {
        Ok(elapsed) => {
            return elapsed.as_millis() as DurationMs;
        }
        Err(e) => {
            panic!("Follow the White Rabbit: {}!", e);
        }
    }
}

struct ScheduleEntry {
    started_at: TimeMs,
    duration: DurationMs
}

type Schedule = Vec<ScheduleEntry>;


// Spending time with fun

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
    let mut triplet_sum: f64;

    for _ in 0..iterations {
    
        triplet = get_next_triplet(triplet);

        triplet_sum = triplet.0 + triplet.1 + triplet.2;
    
        if  (3.14159265 < triplet_sum) && (triplet_sum < 3.14159266) {
            println!("Bingo: {} + {} + {} = {}.", triplet.0, triplet.1, triplet.2, triplet_sum);
        }
    }   
}

fn standard_task(iterations: usize) -> ScheduleEntry {    
    
    let watch = SystemTime::now();
    
    let started_at= now_ms(&watch);

    iterate(random_triplet(), iterations);

    ScheduleEntry{started_at, duration: duration_ms(&watch)}
}


// Performing observations

struct ObservationOutcome {
    mean_task_duration: DurationMs,
    standard_deviation: DurationMs,
    total_duration: DurationMs,
    concurrency_profit: f64,
    task_schedule: Schedule
}

fn count_tasks(oo: &ObservationOutcome) -> usize {
    oo.task_schedule.len()
}

fn last_task(oo: &ObservationOutcome) -> usize {
    count_tasks(oo) - 1
}

fn finished_at(entry: &ScheduleEntry) -> TimeMs {
    entry.started_at + entry.duration
}

fn earliest_start(oo: &ObservationOutcome) -> TimeMs {
    
    let mut es: TimeMs = oo.task_schedule[0].started_at;

    for entry in &oo.task_schedule {  
        if es > entry.started_at {
            es = entry.started_at;
        }
    }    
    
    es    
}

fn latest_finish(oo: &ObservationOutcome) -> TimeMs {

    let mut lf: TimeMs = finished_at(&oo.task_schedule[last_task(oo)]);

    let mut candidate: TimeMs;

    for entry in &oo.task_schedule {
        candidate = finished_at(entry);
        if lf < candidate {
            lf = candidate;
        } 
    }
    
    lf
}

fn total_duration(oo: &ObservationOutcome) -> DurationMs {
    (latest_finish(oo) - earliest_start(oo)) as DurationMs
}

fn sum_duration(oo: &ObservationOutcome) -> DurationMs {

    let mut sum_duration: DurationMs = 0;
    
    for entry in &oo.task_schedule {
        sum_duration += entry.duration;
    }

    sum_duration    
}

fn mean_task_duration(oo: &ObservationOutcome) -> DurationMs {
    sum_duration(oo)/(count_tasks(oo) as TimeCompatibleInt)       
}

fn standard_deviation(oo: &ObservationOutcome) -> DurationMs {

    let mut dispersion: DurationMs = 0;
    let mut deviation: DurationMs;

    for entry in &oo.task_schedule {
        deviation = oo.mean_task_duration - entry.duration;
        dispersion += deviation*deviation;
    }

    ((dispersion as f64).sqrt()/(count_tasks(oo) as f64 - 1.0)) as DurationMs       
}

fn concurrency_profit(oo: &ObservationOutcome, report: &Report) -> f64 {
    if report.len() == 0 {
        return 0.0;
    } else {
        let min_task_duration = report[0].total_duration;
        let queue_total_duration = 
            (count_tasks(oo) as TimeCompatibleInt)*min_task_duration;
        return 1.0 - 
            (oo.total_duration as f64)/(queue_total_duration as f64); 
    }
}

fn observation_outcome(task_schedule: Schedule) -> ObservationOutcome {

    let mut oo = 
        ObservationOutcome {
            mean_task_duration: 0,
            standard_deviation: 0,
            total_duration: 0,
            concurrency_profit: 0f64, 
            task_schedule
        };    
        
    oo.total_duration = total_duration(&oo);
    oo.mean_task_duration = mean_task_duration(&oo);
    oo.standard_deviation = standard_deviation(&oo);

    let earliest_start: TimeMs = earliest_start(&oo);
    for i in 0..oo.task_schedule.len() {
        oo.task_schedule[i].started_at -= earliest_start;
    }
        
    oo    
}

fn fulfil_observation(tasks: usize, iterations: usize) -> ObservationOutcome {

    let mut handles: Vec<ScopedJoinHandle<ScheduleEntry>> = Vec::with_capacity(tasks); 

    crossbeam::scope(|spawner| {
            for _ in 0..tasks {
                handles.push(spawner.spawn(|| {standard_task(iterations)})); 
            }
        }
    );

    let mut task_schedule: Schedule = Vec::with_capacity(handles.capacity());
    for handle in handles {
        task_schedule.push(handle.join());
    }

    observation_outcome(task_schedule)
}


// Printing messages

fn print_help() {
    println!("Commands and arguments");
    println!("Displaying system parameters:");
    println!("s");
    println!("Measuring profits of concurrency:");
    println!("p <Tasks per CPU> <Iterations per task> [Output file]");
}

fn print_report_header() {
    println!("Testing concurrent code execution on Rust");
    println!("");
}

fn print_sysparams_header() {
    println!("============================================================");
    println!("System parameter               Value");
    println!("============================================================");
}

fn print_cpus(cpus: usize) {
    println!("CPUs available {:27}", cpus);
}

fn print_iterations_per_sec(iterations_per_sec: usize) {
    println!("Iterations per second {:>20}", iterations_per_sec.separate_with_commas());
}

fn print_profit_header() {
    println!("============================================================");
    println!("Tasks  Mean task duration  Std. dev.  Total duration  Profit");
    println!("============================================================");
}

fn print_profit_separator() {
    println!("------------------------------------------------------------");
}

fn print_report_footer() {
    println!("============================================================");
}

fn print_profit_entry(oo: &ObservationOutcome) {
    println!("{:5} {:19} {:10} {:15} {:6.0}%", 
             count_tasks(oo),
             oo.mean_task_duration,
             oo.standard_deviation, 
             oo.total_duration, 
             oo.concurrency_profit*100.0);
}


// Formatting and saving a report

fn format_observation_totals_section_header() -> String {
    "Tasks,Mean task duration,Std. dev.,Total duration,Profit\n".to_string()
}

fn format_observation_totals(oo: &ObservationOutcome) -> String {
    format!("{}, {}, {}, {}, {:.0}%\n", 
            count_tasks(oo),
            oo.mean_task_duration,
            oo.standard_deviation,
            oo.total_duration, 
            100.0*oo.concurrency_profit)
}

fn format_observation_totals_section_data(report: &Report) -> String {

    let mut formatted_data: String = "".to_string();

    for oo in report {
        formatted_data += &format_observation_totals(oo);
    }

    formatted_data
} 

fn format_observation_totals_section(report: &Report) -> String {
    format_observation_totals_section_header() + 
    &format_observation_totals_section_data(&report)
}

fn format_observation_schedule_entry(tasks: usize, 
                                     task: usize, 
                                     entry: &ScheduleEntry) -> String {
    format!("{},{},{},{},{}\n", 
            tasks,
            task, 
            entry.started_at, 
            finished_at(entry), 
            entry.duration)
}

fn format_observation_schedule(oo: &ObservationOutcome) -> String {

    let mut schedule_text: String = "".to_string();

    let tasks: usize = count_tasks(&oo);
    let mut task: usize = 1;

    for entry in &oo.task_schedule {
        schedule_text += &format_observation_schedule_entry(tasks, task, entry);
        task += 1;
    }

    schedule_text
}

fn format_observation_schedule_header() -> String {
    "Tasks,Task,Started,Finished,Duration\n".to_string()
}

fn format_observation_schedules_section(report: &Report) -> String {

    let mut section_text: String = format_observation_schedule_header();
    
    for oo in report {
        section_text += &format_observation_schedule(oo);
    }

    section_text
}

fn format_report(report: &Report) -> String {
    format_observation_totals_section(&report) +
    "\n" + 
    &format_observation_schedules_section(&report)
}

fn save_text(file_path: &str, text: &String) {

    if file_path != "" {

        let out_file_path = Path::new(file_path);

        match File::create(out_file_path) {
            Ok(mut out_file) => {
                out_file.write_all(&text.as_bytes()).unwrap();
            }   
            Err(e) => {
                panic!("Error while opening an output file: {}", e);
            }
        }
    } 
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

    let watch = SystemTime::now();
    let mut millis: TimeMs = 0;

    let mut triplet = random_triplet();

    while millis <= 1000 {
        iterations_per_sec += 1;
        triplet = get_next_triplet(triplet);
        millis = duration_ms(&watch);
    }

    iterations_per_sec
}

type Report = Vec<ObservationOutcome>;

fn measure_concurrency_profit(max_tasks_per_cpu: usize, iterations: usize) -> Report {
    
    let mut tasks: usize;

    let mut oo: ObservationOutcome;

    let cpus = count_cpus();

    let mut report: Report = Vec::with_capacity(cpus*max_tasks_per_cpu);
            
    print_profit_header();

    for tasks_per_cpu in 0..max_tasks_per_cpu {

        for cpu in 0..cpus {
            tasks = 1 + cpu + tasks_per_cpu*cpus;
            oo = fulfil_observation(tasks, iterations);
            oo.concurrency_profit = concurrency_profit(&oo, &report);
            print_profit_entry(&oo);
            report.push(oo);
        }

        if tasks_per_cpu < max_tasks_per_cpu - 1 {
            print_profit_separator();
        }
    } 

    print_report_footer();

    report
}


// Accepting arguments

type ArgsVec = Vec<String>;

#[derive(PartialEq)]
enum Command {
    Help,
    RequestSysParams,
    MeasureConcurrencyProfit,
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
                let report = measure_concurrency_profit(
                    accept_tasks_per_cpu(&args),
                    accept_iterations(&args));
                save_text(accept_out_file_path(&args), &format_report(&report));
            } else {
                print_help();
            }
        }
    }
}