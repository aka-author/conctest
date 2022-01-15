// * * ** *** ***** ******** ************* *********************
// Observing concurent code execution in Rust
// * * ** *** ***** ******** ************* *********************

use std::time::*;
use num_cpus;
use rand;


// Retrieving system parameters

fn count_cpus() -> usize {
    return num_cpus::get()
}


// Spending time

fn random_sequence_member() -> isize {
    let seed:f64 = rand::random();
    return (seed*10f64).floor() as isize;
}

fn complex_task(number_of_iterations: usize) {

    let mut r1: isize = random_sequence_member();
    let mut r2: isize = random_sequence_member();
    let mut r3: isize = random_sequence_member();
    let mut r4: isize;

    for _i in 0..number_of_iterations {
        r4 = r1 + r2 - r3; 
        r1 = r2;
        r2 = r3;
        r3 = r4;
    }
}

fn get_number_of_iterations() -> usize {

    let mut iterations_per_10ms: usize = 0;

    let clock = SystemTime::now();
    let mut mills: u128 = 0;

    while mills <= 10 {

        complex_task(1);
        
        match clock.elapsed() {
            Ok(elapsed) => {
                iterations_per_10ms += 1; 
                mills = elapsed.as_millis();
            }
            Err(_e) => {
                mills = 10;
            }
        }
    }

    return iterations_per_10ms*1000;
}


// Performing observations

fn fulfil_observation(number_of_tasks: usize, number_of_iterations: usize) -> u128 {

    let clock = SystemTime::now();

    crossbeam::scope(|spawner| {
            
            for _task_idx in 0..number_of_tasks {
                spawner.spawn(|| {complex_task(number_of_iterations)});  
            }
        }
    );

    match clock.elapsed() {
        Ok(elapsed) => {
            return elapsed.as_millis();
        }
        Err(_e) => {
            return 0;
        }
    }
}

fn measure_base_duration(number_of_iterations: usize) -> u128 {

    let number_of_trys = 10;

    let mut sumdur: u128 = 0;

    for _i in 0..number_of_trys {
        sumdur += fulfil_observation(1, number_of_iterations);
    }

    return sumdur/number_of_trys;
}


// Printing a report

fn print_report_header(number_of_cpus: usize) {
    println!("Testing concurent code execution in Rust.");
    println!("Number of CPUs in the system: {}.", number_of_cpus);
}

fn print_report_table_header() {
    println!("==========================================");
    println!("Tasks  Duration  Relative duration  Profit");
    println!("==========================================");
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

    let number_of_cpus = count_cpus();

    let number_of_iterations = get_number_of_iterations();
    
    print_report_header(number_of_cpus);
    
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

    print_report_table_footer();
    println!("{} ", number_of_iterations);
}
