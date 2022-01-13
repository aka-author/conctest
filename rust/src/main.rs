// * * ** *** ***** ******** ************* *********************
// Observing concurent code execution in Rust
// * * ** *** ***** ******** ************* *********************

use std::time::*;
use num_cpus;


// Retrieving system parameters

fn count_cpus() -> usize {
    return num_cpus::get()
}


// Spending time

fn complex_task()  {

    let mut _k: f64;

    for _i in 0..5000000 {
        _k = 2636625362.0/2763.0;
    }
}


// Performing observations

fn fulfil_observation(number_of_cpus: usize) -> u128 {

    let clock = SystemTime::now();

    crossbeam::scope(|spawner| {
            
            for _cpu_idx in 0..number_of_cpus {
                spawner.spawn(|| {complex_task()});  
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

fn measure_base_duration() -> u128 {

    let number_of_iterations = 10;

    let mut sumdur: u128 = 0;

    for _i in 0..number_of_iterations {
        sumdur += fulfil_observation(1);
    }

    return sumdur/number_of_iterations;
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

    print_report_header(number_of_cpus);
    
    let mut number_of_tasks: usize;
    let mut duration: u128;
    
    let base_duration = measure_base_duration();

    print_report_table_header();

    for layer in 0..3 {
        for cpu in 0..number_of_cpus {
            number_of_tasks = 1 + cpu + layer*number_of_cpus;
            duration = fulfil_observation(number_of_tasks);
            print_report_table_entry(number_of_tasks, base_duration, duration);
        }

        print_report_table_separator();
    }

    number_of_tasks = number_of_cpus*10;
    duration = fulfil_observation(number_of_tasks);
    print_report_table_entry(number_of_tasks, base_duration, duration);

    print_report_table_footer();
}
