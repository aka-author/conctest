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


// Measuring time and working with schedules

type TimeMs = i128;
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

fn duration_ms(clock: &SystemTime) -> TimeMs {
    match clock.elapsed() {
        Ok(elapsed) => {
            return elapsed.as_millis() as TimeMs;
        }
        Err(e) => {
            panic!("Follow the White Rabbit: {}!", e);
        }
    }
}

struct ScheduleEntry {
    started_at: TimeMs,
    duration: TimeMs
}

impl ScheduleEntry {

    fn create_sd(started_at: TimeMs, duration: TimeMs) -> ScheduleEntry {
        ScheduleEntry{started_at, duration}
    }

    fn get_started_at(self: &Self) -> TimeMs {
        self.started_at
    }

    fn recalc_start_from(self: &mut Self, initial_moment: TimeMs) {
        self.started_at -= initial_moment
    }

    fn get_finished_at(self: &Self) -> TimeMs {
        self.started_at + self.duration
    }
    
    fn get_duration(self: &Self) -> TimeMs {
        self.duration
    }
}

type Schedule = Vec<ScheduleEntry>;

/*
impl Schedule {

    fn add_entry(self: &mut Self, started_at: TimeMs, duration: TimeMs) {
        self.push(ScheduleEntry{started_at, duration});    
    }

    fn earliest_start(self: &Self) -> TimeMs {

    }

    fn latest_finish(self: &Self) -> TimeMs {
        
    }

}*/


// Spending time with fun

type Triplet = (f64, f64, f64);

fn random_item() -> f64 {    
    rand::random()
}

fn random_triplet() -> Triplet {
    (random_item(), random_item(), random_item())
}

fn get_next_triplet(triplet: Triplet) -> Triplet {

    let mut next_member = triplet.0 + triplet.1 - triplet.2;

    if next_member.abs() > 1.0 {
        next_member = 1.0/next_member;
    }

    (triplet.1, triplet.2, next_member)
}

fn is_bingo(member: f64) -> bool {
    -0.00000000000001 < member && member < 0.00000000000001
}

fn iterate(initial_triplet: Triplet, cycles: usize) -> f64 {

    let mut triplet = initial_triplet;
    
    for step in 0..cycles {
    
        triplet = get_next_triplet(triplet);

        if is_bingo(triplet.2) {
            print_bingo(initial_triplet, step, triplet.2);
        }
    }    

    triplet.2
}

fn standard_task(cycles: usize) -> ScheduleEntry {    
    
    let watch = SystemTime::now();
    
    let started_at= now_ms(&watch);

    iterate(random_triplet(), cycles);

    ScheduleEntry::create_sd(started_at, duration_ms(&watch))
}


// Performing observations

struct Observation {
    mean_task_duration: TimeMs,
    standard_deviation: TimeMs,
    total_duration: TimeMs,
    concurrency_profit: f64,
    task_schedule: Schedule
}

fn count_tasks(oo: &Observation) -> usize {
    oo.task_schedule.len()
}

fn last_task(oo: &Observation) -> usize {
    count_tasks(oo) - 1
}

fn earliest_start(oo: &Observation) -> TimeMs {
    
    let mut es: TimeMs = oo.task_schedule[0].get_started_at();

    for entry in &oo.task_schedule {  
        if es > entry.get_started_at() {
            es = entry.get_started_at();
        }
    }    
    
    es    
}

fn latest_finish(oo: &Observation) -> TimeMs {

    let mut lf: TimeMs = oo.task_schedule[last_task(oo)].get_finished_at();

    let mut candidate: TimeMs;

    for entry in &oo.task_schedule {
        candidate = entry.get_finished_at();
        if lf < candidate {
            lf = candidate;
        } 
    }
    
    lf
}

fn total_duration(oo: &Observation) -> TimeMs {
    (latest_finish(oo) - earliest_start(oo)) as TimeMs
}

fn sum_duration(oo: &Observation) -> TimeMs {

    let mut sum_duration: TimeMs = 0;
    
    for entry in &oo.task_schedule {
        sum_duration += entry.get_duration();
    }

    sum_duration    
}

fn mean_task_duration(oo: &Observation) -> TimeMs {
    sum_duration(oo)/(count_tasks(oo) as TimeCompatibleInt)       
}

fn standard_deviation(oo: &Observation) -> TimeMs {

    let mut dispersion: TimeMs = 0;
    let mut deviation: TimeMs;

    for entry in &oo.task_schedule {
        deviation = oo.mean_task_duration - entry.get_duration();
        dispersion += deviation*deviation;
    }

    ((dispersion as f64).sqrt()/(count_tasks(oo) as f64 - 1.0)) as TimeMs       
}

fn concurrency_profit(oo: &Observation, report: &Report) -> f64 {
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

fn observation(task_schedule: Schedule) -> Observation {

    let mut oo = 
        Observation {
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
        oo.task_schedule[i].recalc_start_from(earliest_start);
    }
        
    oo    
}

fn count_series(tasks: usize, series_size: usize) -> usize {
    if tasks % series_size == 0 {
        return tasks/series_size;
    } else {
        return tasks/series_size + 1;
    }
}

fn observe(tasks: usize, cycles: usize, series_size: usize) -> Observation {

    let series = count_series(tasks, series_size);
    let mut count_tasks_total = 0usize;
    let mut count_tasks_series = 0usize;
    let mut handles: Vec<ScopedJoinHandle<ScheduleEntry>> = Vec::with_capacity(tasks); 

    for _ in 0..series { 
        crossbeam::scope(|spawner| {
            count_tasks_series = 0;
            while count_tasks_total < tasks && count_tasks_series < series_size {
                handles.push(spawner.spawn(|| {standard_task(cycles)}));
                count_tasks_series += 1;
                count_tasks_total += 1;
            }
        });
    }

    let mut task_schedule: Schedule = Vec::with_capacity(handles.capacity());
    for handle in handles {
        task_schedule.push(handle.join());
    }

    observation(task_schedule)
}


// Getting parameters of the current system

fn count_cpus() -> usize {
    num_cpus::get()
}

fn count_cycles_per_sec() -> usize {

    let mut duration: TimeMs = 0;    
    let mut cycles: usize = 1; 

    while duration < 1000 {
        cycles *= 10;
        let watch = SystemTime::now();
        iterate(random_triplet(), cycles);
        duration = duration_ms(&watch);
    }

    (1000*cycles as TimeCompatibleInt/duration) as usize
}


// Printing messages to a console

fn print_salutation() {
    println!("Testing concurrent code execution on Rust\n");
}

fn print_help() {
    println!("Commands and arguments");
    println!("Displaying system parameters:");
    println!("s");
    println!("Measuring profits of concurrency:");
    println!("p <Number of tasks> <Cycles in a task> <Tasks in a series> [Output file]");
}

fn print_sysparams_header() {
    println!("====================================");
    println!("System parameter               Value");
    println!("====================================");
}

fn print_cpus(cpus: usize) {
    println!("CPUs available {:21}", cpus);
}

fn print_cycles_per_sec(cycles_per_sec: usize) {
    println!("Cycles per second {:>18}", cycles_per_sec.separate_with_commas());
}

fn print_sysparams_footer() {
    println!("====================================");
}

fn print_profit_header() {
    println!("============================================================");
    println!("Tasks  Mean task duration  Std. dev.  Total duration  Profit");
    println!("============================================================");
}

fn print_profit_entry(oo: &Observation) {
    println!("{:5} {:19} {:10} {:15} {:6.0}%", 
             count_tasks(oo),
             oo.mean_task_duration,
             oo.standard_deviation, 
             oo.total_duration, 
             oo.concurrency_profit*100.0);
}

fn print_bingo(initial_triplet: Triplet, step: usize, member: f64) {
    println!("Bingo: {}, {}, and {} give {} on step {}.", 
             initial_triplet.0, 
             initial_triplet.1, 
             initial_triplet.2, 
             member, 
             step.separate_with_commas());
}

fn print_profit_separator() {
    println!("------------------------------------------------------------");
}

fn print_profit_footer() {
    println!("============================================================");
}


// Formatting and saving a report

fn format_observation_totals_section_header() -> String {
    "Tasks,Mean task duration,Std. dev.,Total duration,Profit\n".to_string()
}

fn format_observation_totals(oo: &Observation) -> String {
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
            entry.get_started_at(), 
            entry.get_finished_at(), 
            entry.get_duration())
}

fn format_observation_schedule(oo: &Observation) -> String {

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

fn save_text(out_file_path: &str, text: &String) {

    if out_file_path != "" {
        match File::create(Path::new(out_file_path)) {
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

fn test_sysparams() {
    print_sysparams_header();
    print_cpus(count_cpus());
    print_cycles_per_sec(count_cycles_per_sec());
    print_sysparams_footer();
}

type Report = Vec<Observation>;

fn test_concurrency_profit(tasks_max: usize, cycles: usize, series_size: usize) -> Report {
    
    let mut report: Report = Vec::with_capacity(tasks_max);

    print_profit_header();

    let mut oo: Observation;

    for tasks in 1..tasks_max + 1 {

        oo = observe(tasks, cycles, series_size);
        oo.concurrency_profit = concurrency_profit(&oo, &report);
        
        print_profit_entry(&oo);
        if tasks % count_cpus() == 0 && tasks != tasks_max {
            print_profit_separator();
        }

        report.push(oo);
    } 

    print_profit_footer();

    report
}


// Accepting arguments

fn validate_usize(s: &str) -> bool {   
    Regex::new(r"^\d+$").unwrap().is_match(&s)
}

fn parse_usize(s: &String) -> usize {
    if validate_usize(s) {
        return s.parse::<usize>().unwrap();
    } else {
        return 0;
    }    
}

type ArgsVec = Vec<String>;

#[derive(Copy, Clone, PartialEq)]
enum Command {
    Help,
    RequestSysParams,
    MeasureConcurrencyProfit,
}

const ARG_IDX_COMMAND: usize = 1;
const ARG_IDX_TASKS: usize = 2;
const ARG_IDX_CYCLES: usize = 3;
const ARG_IDX_SERIES_SIZE: usize = 4;
//const ARG_IDX_OUT_FILE_PATH: usize = 5;

struct Args {
    command: Command,
    tasks: usize,
    cycles: usize,
    series_size: usize
    //out_file_path: &'static String
}

impl Args {

    fn get_command(self: &Self) -> Command {
        self.command
    }

    fn get_tasks(self: &Self) -> usize {
        self.tasks
    }

    fn get_cycles(self: &Self) -> usize {
        self.cycles
    }

    fn get_series_size(self: &Self) -> usize {
        self.series_size
    }

    /*fn get_out_file_path(self: &Self) -> &String {
        self.out_file_path
    }*/

    fn parse_command(self: &Self, args: &ArgsVec) -> Command {

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

    fn parse_tasks(self: &Self, args: &ArgsVec) -> usize {
        parse_usize(&args[ARG_IDX_TASKS])
    }

    fn parse_cycles(self: &Self, args: &ArgsVec) -> usize {
        parse_usize(&args[ARG_IDX_CYCLES])
    }
    
    fn parse_series_size(self: &Self, args: &ArgsVec) -> usize {
        parse_usize(&args[ARG_IDX_SERIES_SIZE])
    }
    
    /*fn parse_out_file_path(self: &Self, args: &ArgsVec) -> &String {
        if args.len() == ARG_IDX_OUT_FILE_PATH + 1 {
            return &"foo.txt".to_string(); //&args[ARG_IDX_OUT_FILE_PATH];
        } else {
            return &"".to_string();
        }
    }*/
    
    fn parse(mut self: Self, args: &ArgsVec) -> Self {

        if args.len() >= 1 {
            self.command = self.parse_command(args);
            if args.len() >= 4 {
                self.tasks = self.parse_tasks(args);
                self.cycles = self.parse_cycles(args);
                self.series_size = self.parse_series_size(args);
            }
            //self.out_file_path = &self.parse_out_file_path(args);
        }

        self
    }

    fn is_valid(self: &Self) -> bool {
        self.get_tasks() > 0 &&
        self.get_cycles() > 0 &&
        self.get_series_size() > 0 && 
        self.get_series_size() <= self.get_tasks()
    }
}

fn accept_args(args: ArgsVec) -> Args {
    Args{command: Command::Help, 
         tasks: 0, 
         cycles: 0, 
         series_size: 0}.parse(&args) 
         //out_file_path: &"".to_string()}.parse(&args)
}


// Doing the job 

fn main() {

    print_salutation();

    let args: Args = accept_args(env::args().collect());

    match args.get_command() {
        Command::Help => {
            print_help();
        }
        Command::RequestSysParams => {
            test_sysparams();
        }
        Command::MeasureConcurrencyProfit => {
            if args.is_valid() {
                let report = test_concurrency_profit(
                    args.get_tasks(),
                    args.get_cycles(), 
                    args.get_series_size());
                save_text("report.txt", &format_report(&report));
            } else {
                print_help();
            }
        }
    }
}