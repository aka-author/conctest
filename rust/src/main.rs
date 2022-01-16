// * * ** *** ***** ******** ************* *********************
// Observing concurent code execution on Rust
// * * ** *** ***** ******** ************* *********************

use std::env;
use std::panic;
use std::time::*;
use num_cpus;
use rand;
use thousands::Separable;


// Retrieving system parameters

fn count_cpus() -> usize {
    num_cpus::get()
}


// Spending time

type MemberTriplet = (isize, isize, isize);

fn random_member() -> isize {
    let seed:f64 = rand::random();
    (seed*10f64).floor() as isize
}

fn get_next_member(triplet: MemberTriplet) -> MemberTriplet {
    (triplet.1, triplet.2, triplet.0 + triplet.1 - triplet.2)
}

fn random_triplet() -> MemberTriplet {
    (random_member(), random_member(), random_member())
}

fn generate_members(initial_triplet: MemberTriplet, number_of_members: usize) {

    let mut triplet = initial_triplet;

    for _i in 0..number_of_members {
        triplet = get_next_member(triplet);
    }    
}

fn complex_task(number_of_members: usize) {
    generate_members(random_triplet(), number_of_members)
}


// Performing observations

fn fulfil_observation(number_of_tasks: usize, number_of_members: usize) -> u128 {

    let clock = SystemTime::now();

    crossbeam::scope(|spawner| {
            
            for _task_idx in 0..number_of_tasks {
                spawner.spawn(|| {complex_task(number_of_members)});  
            }
        }
    );

    match clock.elapsed() {
        Ok(elapsed) => {
            return elapsed.as_millis();
        }
        Err(e) => {
            panic!("Clock runtime error: {}.", e);
        }
    }
}

fn measure_members_per_sec() -> usize {

    let mut members_per_sec: usize = 0;

    let clock = SystemTime::now();
    let mut mills: u128 = 0;

    let mut triplet: MemberTriplet = (1, 2, 3);

    while mills <= 1000 {

        triplet = get_next_member(triplet);
        
        match clock.elapsed() {
            Ok(elapsed) => {
                members_per_sec += 1; 
                mills = elapsed.as_millis();
            }
            Err(e) => {
                panic!("Clock runtime error: {}.", e);
            }
        }
    }

    members_per_sec
}

fn measure_base_duration(number_of_members: usize) -> u128 {

    let number_of_observations = 100;

    let mut total_duration: u128 = 0;

    for _i in 0..number_of_observations {
        total_duration += fulfil_observation(1, number_of_members);
    }

    total_duration/number_of_observations
}


// Printing a report

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

fn print_number_of_cpus(number_of_cpus: usize) {
    println!("CPUs available {:27}", number_of_cpus);
}

fn print_members_per_sec(members_per_sec: usize) {
    println!("Iterations per second {:>20}", members_per_sec.separate_with_commas());
}

fn print_report_table_entry(number_of_tasks: usize, base_duration: u128, duration: u128) {
    let k = duration as f32/base_duration as f32;
    let linear_duration = number_of_tasks as u128 * base_duration;
    let profit = 100*(linear_duration as i128 - duration as i128)/linear_duration as i128;
    println!("{:5} {:9} {:18.3} {:6}%", number_of_tasks, duration, k, profit);
}

fn print_report_table_separator() {
    println!("------------------------------------------");
}

fn print_report_table_footer() {
    println!("==========================================");
}


// Performing observations and printing a report

fn main() {

    print_report_header();

    let args: Vec<String> = env::args().collect();

    let number_of_cpus = count_cpus();

    if args.len() == 1 {
        print_report_sysparams_header();
        print_number_of_cpus(number_of_cpus);
        let members_per_sec = measure_members_per_sec();
        print_members_per_sec(members_per_sec);
    } else {

        let number_of_iterations = 1_500_000;
        
        let mut number_of_tasks: usize;
        let mut duration: u128;
        
        let base_duration = measure_base_duration(number_of_iterations);

        print_report_table_header();

        for layer in 0..3 {
            for cpu in 0..number_of_cpus {
                number_of_tasks = 1 + cpu + layer*number_of_cpus;
                duration = fulfil_observation(number_of_tasks, number_of_iterations);
                print_report_table_entry(number_of_tasks, base_duration, duration);
            }

            print_report_table_separator();
        }

        number_of_tasks = number_of_cpus*10;
        duration = fulfil_observation(number_of_tasks, number_of_iterations);
        print_report_table_entry(number_of_tasks, base_duration, duration);
    }

    print_report_table_footer();
}