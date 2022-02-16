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


// Measuring time

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

fn duration_ms(watch: &SystemTime) -> TimeMs {
    match watch.elapsed() {
        Ok(elapsed) => {
            return elapsed.as_millis() as TimeMs;
        }
        Err(e) => {
            panic!("Follow the White Rabbit: {}!", e);
        }
    }
}


// Spending time with fun

type Triplet = (f64, f64, f64);

fn random_item() -> f64 {    
    rand::random()
}

fn random_triplet() -> Triplet {
    (random_item(), random_item(), random_item())
}

fn get_next_triplet(triplet: Triplet) -> Triplet {

    let applicant = triplet.0 + triplet.1 - triplet.2;

    if applicant.abs() <= 1.0 {
        return (triplet.1, triplet.2, applicant);
    } else {
        return (triplet.1, triplet.2, 1.0/applicant);
    }
}

fn approx_eq(f1: f64, f2: f64) -> bool {
    return (f1 - f2).abs() < 0.00000000000001
}

fn is_convergent(triplet: Triplet, next_triplet: Triplet) -> bool {
    approx_eq(triplet.0, next_triplet.0) &&
    approx_eq(triplet.1, next_triplet.1) &&
    approx_eq(triplet.2, next_triplet.2)
}

fn iterate(initial_triplet: Triplet, n_cycles: usize) -> f64 {
    
    let mut triplet = initial_triplet;

    let mut prokukarek = false;

    for step in 0..n_cycles {
    
        let next_triplet = get_next_triplet(triplet);

        if is_convergent(triplet, next_triplet) && !prokukarek {
            print_convergency(initial_triplet, step, triplet.2);
            prokukarek = true;
        }

        triplet = next_triplet;
    }    

    triplet.2
}

fn standard_task(n_cycles: usize) -> Task {     
    let watch = SystemTime::now();
    let start= now_ms(&watch);
    iterate(random_triplet(), n_cycles);
    Task::create(start, duration_ms(&watch))
}


// Managing observation outcomes

struct Task {
    start: TimeMs,
    duration: TimeMs
}

impl Task {

    fn get_start(self: &Self) -> TimeMs {
        self.start
    }

    fn recalc_start_relative(self: &mut Self, initial_moment: TimeMs) {
        self.start -= initial_moment
    }

    fn get_finish(self: &Self) -> TimeMs {
        self.start + self.duration
    }
    
    fn get_duration(self: &Self) -> TimeMs {
        self.duration
    }

    fn create(start: TimeMs, duration: TimeMs) -> Task {
        Task{start, duration}
    }
}

struct Observation {
    tasks: Vec<Task>,
    concurrency_profit: f64
}

impl Observation {

    fn register_task(self: &mut Self, task: Task) {
        self.tasks.push(task);    
    }

    fn count_tasks(self: &Self) -> usize {
        self.tasks.len()
    }

    fn get_earliest_start(self: &Self) -> TimeMs {
        self.tasks.iter().map(|task| task.get_start()).min().unwrap()
    }

    fn get_latest_finish(self: &Self) -> TimeMs {
        self.tasks.iter().map(|task| task.get_finish()).max().unwrap()
    }

    fn recalc_tasks_relative_earliest_start(self: &mut Self) {

        let initial_moment = self.get_earliest_start();

        for task in &mut self.tasks {
            task.recalc_start_relative(initial_moment);
        }
    }

    fn get_total_duration(self: &Self) -> TimeMs {
        (self.get_latest_finish() - self.get_earliest_start()) as TimeMs
    }

    fn sum_duration(self: &Self) -> TimeMs {
        let mut sum: TimeMs = 0;
        self.tasks.iter().for_each(|task| sum += task.get_duration());
        sum    
    }
    
    fn get_mean_task_duration(self: &Self) -> TimeMs {
        self.sum_duration()/(self.count_tasks() as TimeCompatibleInt)       
    }
    
    fn get_standard_deviation(self: &Self) -> TimeMs {
    
        let mut dispersion: TimeMs = 0;
        let mut deviation: TimeMs;

        let mean_task_duration = self.get_mean_task_duration();
    
        for task in &self.tasks {
            deviation = mean_task_duration - task.get_duration();
            dispersion += deviation*deviation;
        }
    
        ((dispersion as f64).sqrt()/(self.count_tasks() as f64 - 1.0)) as TimeMs       
    }
    
    fn get_concurrency_profit(self: &Self) -> f64 {
        self.concurrency_profit
    }    

    fn calc_concurrency_profit(self: &mut Self, task_duration_min: TimeMs) -> f64 {
        
        let serial_duration = 
            (self.count_tasks() as TimeCompatibleInt)*task_duration_min;
        
        self.concurrency_profit = 
            1.0 - (self.get_total_duration() as f64)/(serial_duration as f64);

        return self.concurrency_profit
    }

    fn with_capacity(capacity: usize) -> Observation {

        Observation {
            tasks: Vec::with_capacity(capacity),
            concurrency_profit: 0f64 
        }
    }   
}

struct Report {
    observations: Vec<Observation>    
}

impl Report {

    fn count_observations(self: &Self) -> usize {
        self.observations.len()
    }

    fn get_task_duration_min(self: &Self) -> TimeMs {
        self.observations[0].get_total_duration()
    }

    fn register_observation(self: &mut Self, mut obs: Observation) {
        
        if self.count_observations() > 0 {
            obs.calc_concurrency_profit(self.get_task_duration_min());
        }

        obs.recalc_tasks_relative_earliest_start();

        self.observations.push(obs);
    }

    fn get_observation(self: &Self, idx: usize) -> &Observation {
        &(self.observations[idx])
    }

    fn create(ntasks_max: usize) -> Report {
        Report {
            observations: Vec::with_capacity(ntasks_max)
        }
    }
}


// Performing observations

fn count_series(n_tasks: usize, series_size: usize) -> usize {

    let mut n_series = n_tasks/series_size;

    if series_size*n_series < n_tasks {
        n_series += 1;
    }

    n_series
}

fn observe(n_tasks: usize, n_cycles: usize, series_size: usize) -> Observation {

    let n_series = count_series(n_tasks, series_size);
    let mut count_tasks_total = 0usize;
    let mut count_tasks_series = 0usize;
    let mut handles: Vec<ScopedJoinHandle<Task>> = Vec::with_capacity(n_tasks); 

    for _ in 0..n_series { 
        crossbeam::scope(|spawner| {
            count_tasks_series = 0;
            while count_tasks_total < n_tasks && count_tasks_series < series_size {
                handles.push(spawner.spawn(|| {standard_task(n_cycles)}));
                count_tasks_series += 1;
                count_tasks_total += 1;
            }
        });
    }

    let mut obs = Observation::with_capacity(handles.capacity());
    for handle in handles {
        obs.register_task(handle.join());
    }

    obs
}


// Getting parameters of the current system

fn count_cpus() -> usize {
    num_cpus::get()
}

fn count_cycles_per_sec() -> usize {

    let mut duration: TimeMs = 0;    
    let mut n_cycles: usize = 1; 

    while duration < 1000 {
        n_cycles *= 10;
        let watch = SystemTime::now();
        iterate(random_triplet(), n_cycles);
        duration = duration_ms(&watch);
    }

    (1000*n_cycles as TimeCompatibleInt/duration) as usize
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

fn print_cpus(n_cpus: usize) {
    println!("CPUs available {:>21}", n_cpus);
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

fn print_profit_entry(obs: &Observation) {
    println!("{:5} {:19} {:10} {:15} {:6.0}%", 
             obs.count_tasks(),
             obs.get_mean_task_duration(),
             obs.get_standard_deviation(), 
             obs.get_total_duration(), 
             obs.get_concurrency_profit()*100.0);
}

fn print_convergency(initial_triplet: Triplet, step: usize, member: f64) {
    println!("The sequence has converged: {}, {}, and {} give {} since step {}.", 
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

fn format_observation_totals(obs: &Observation) -> String {
    format!("{}, {}, {}, {}, {:.0}%\n", 
            obs.count_tasks(),
            obs.get_mean_task_duration(),
            obs.get_standard_deviation(),
            obs.get_total_duration(), 
            obs.get_concurrency_profit()*100.0)
}

fn format_observation_totals_section_data(report: &Report) -> String {

    let mut formatted_data: String = "".to_string();

    for obs in &report.observations {
        formatted_data += &format_observation_totals(obs);
    }

    formatted_data
} 

fn format_observation_totals_section(report: &Report) -> String {
    format_observation_totals_section_header() + 
    &format_observation_totals_section_data(&report)
}

fn format_task(n_tasks: usize, task_idx: usize, task: &Task) -> String {
    format!("{},{},{},{},{}\n", 
            n_tasks,
            task_idx, 
            task.get_start(), 
            task.get_finish(), 
            task.get_duration())
}

fn format_tasks(obs: &Observation) -> String {

    let mut schedule_text: String = "".to_string();

    let n_tasks: usize = obs.count_tasks();
    let mut task_idx: usize = 1;

    for task in &obs.tasks {
        schedule_text += &format_task(n_tasks, task_idx, task);
        task_idx += 1;
    }

    schedule_text
}

fn format_observation_schedule_header() -> String {
    "Tasks,Task,Started,Finished,Duration\n".to_string()
}

fn format_observation_schedules_section(report: &Report) -> String {

    let mut section_text: String = format_observation_schedule_header();
    
    for obs in &report.observations {
        section_text += &format_tasks(obs);
    }

    section_text
}

fn format_report(report: &Report) -> String {
    format_observation_totals_section(&report) +
    "\n" + 
    &format_observation_schedules_section(&report)
}

fn save_text(out_file_path: &String, text: &String) {

    if *out_file_path != "".to_string() {
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

fn test_concurrency_profit(tasks_max: usize, n_cycles: usize, series_size: usize) -> Report {
    
    let mut report = Report::create(tasks_max);

    print_profit_header();

    for n_tasks in 1..tasks_max + 1 {

        let obs = observe(n_tasks, n_cycles, series_size);

        report.register_observation(obs);
        
        print_profit_entry(report.get_observation(n_tasks - 1));
        if n_tasks % count_cpus() == 0 && n_tasks != tasks_max {
            print_profit_separator();
        }    
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
const ARG_IDX_TASKS_MAX: usize = 2;
const ARG_IDX_N_CYCLES: usize = 3;
const ARG_IDX_SERIES_SIZE: usize = 4;
const ARG_IDX_OUT_FILE_PATH: usize = 5;

struct Args {
    command: Command,
    tasks_max: usize,
    n_cycles: usize,
    series_size: usize,
    out_file_path: String
}

impl Args {

    fn get_command(self: &Self) -> Command {
        self.command
    }

    fn get_tasks_max(self: &Self) -> usize {
        self.tasks_max
    }

    fn get_n_cycles(self: &Self) -> usize {
        self.n_cycles
    }

    fn get_series_size(self: &Self) -> usize {
        self.series_size
    }

    fn get_out_file_path(self: &Self) -> String {
        self.out_file_path.clone()
    }

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

    fn parse_tasks_max(self: &Self, args: &ArgsVec) -> usize {
        parse_usize(&args[ARG_IDX_TASKS_MAX])
    }

    fn parse_n_cycles(self: &Self, args: &ArgsVec) -> usize {
        parse_usize(&args[ARG_IDX_N_CYCLES])
    }
    
    fn parse_series_size(self: &Self, args: &ArgsVec) -> usize {
        parse_usize(&args[ARG_IDX_SERIES_SIZE])
    }
    
    fn parse_out_file_path(self: &Self, args: &ArgsVec) -> String {
        if args.len() == ARG_IDX_OUT_FILE_PATH + 1 {
            return args[ARG_IDX_OUT_FILE_PATH].to_string(); 
        } else {
            return "".to_string();
        }
    }
    
    fn parse(mut self: Self, args: &ArgsVec) -> Self {

        if args.len() >= 1 {
            self.command = self.parse_command(args);
            if args.len() >= 4 {
                self.tasks_max = self.parse_tasks_max(args);
                self.n_cycles = self.parse_n_cycles(args);
                self.series_size = self.parse_series_size(args);
            }
            self.out_file_path = self.parse_out_file_path(args);
        }

        self
    }

    fn is_valid(self: &Self) -> bool {
        self.get_tasks_max() > 0 &&
        self.get_n_cycles() > 0 &&
        self.get_series_size() > 0 && 
        self.get_series_size() <= self.get_tasks_max()
    }
}

fn accept_args(args: ArgsVec) -> Args {
    Args{command: Command::Help, 
         tasks_max: 0, 
         n_cycles: 0, 
         series_size: 0, 
         out_file_path: "".to_string()}.parse(&args)
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
                    args.get_tasks_max(),
                    args.get_n_cycles(), 
                    args.get_series_size());
                save_text(&args.get_out_file_path(), &format_report(&report));
            } else {
                print_help();
            }
        }
    }
}