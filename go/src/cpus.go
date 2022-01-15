package main

import (
	"fmt"
	"runtime"
)

// Retrieving system parameters

func count_cpus() int {
	return runtime.NumCPU()
}

// Printing a report

func print_report_header(number_of_cpus int) {
	fmt.Println("Testing concurent code execution in Go.")
	fmt.Printf("Number of CPUs in the system: %v.\n", number_of_cpus)
}

/*
func print_report_table_header() {
    fmt.Println("==========================================");
    fmt.Println("Tasks  Duration  Relative duration  Profit");
    fmt.Println("==========================================");
}

func print_report_table_entry(number_of_tasks, base_duration, duration: uint) {
    k := duration/base_duration;
    linear_duration := number_of_tasks*base_duration;
    profit := 100*(linear_duration - duration)/linear_duration;

	// "{:5} {:9} {:18.3} {:6}%", number_of_tasks, duration, k, profit

    fmt.Printf("%5 %9 %18.3f %6", number_of_tasks, duration, k, profit)
}

func print_report_table_separator() {
    fmt.Println("------------------------------------------");
}

func print_report_table_footer() {
    println!("==========================================");
}
*/

func main() {

	number_of_cpus := count_cpus()

	print_report_header(number_of_cpus)

}
