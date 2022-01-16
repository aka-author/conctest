package main

import (
	"fmt"
	"runtime"
	"sync"
	"time"
)

// Retrieving system parameters

func count_cpus() int {
	return runtime.NumCPU()
}

// Spending time

func complex_task() float32 {

	var k float32

	for i := 0; i < 500000000; i++ {
		k += 2636625362.0 / 2763.0
	}

	//fmt.Println(k)

	return k
}

// Performing observations

func fulfil_observation(number_of_cpus int) int {

	var syncler sync.WaitGroup

	time_start := time.Now()

	for i := 0; i < number_of_cpus; i++ {
		syncler.Add(1)
		go func() {
			_ = complex_task()
			syncler.Done()
		}()
	}

	syncler.Wait()

	time_finish := time.Now()
	duration := time_finish.Sub(time_start)

	return int(duration.Milliseconds())
}

func measure_base_duration() int {

	number_of_iterations := 10

	sumdur := 0

	for i := 0; i < number_of_iterations; i++ {
		sumdur += fulfil_observation(1)
	}

	return sumdur / number_of_iterations
}

// Printing a report

func print_report_header(number_of_cpus int) {
	fmt.Println("Testing concurent code execution in Go.")
	fmt.Printf("Number of CPUs in the system: %v.\n", number_of_cpus)
}

func print_report_table_header() {
	fmt.Println("==========================================")
	fmt.Println("Tasks  Duration  Relative duration  Profit")
	fmt.Println("==========================================")
}

func print_report_table_entry(number_of_tasks, base_duration, duration int) {
	k := float32(duration) / float32(base_duration)
	linear_duration := number_of_tasks * base_duration
	profit := 100 * (linear_duration - duration) / linear_duration

	// "{:5} {:9} {:18.3} {:6}%", number_of_tasks, duration, k, profit

	fmt.Printf("%5d %9d %18.3f %6d\n", number_of_tasks, duration, k, profit)
}

func print_report_table_separator() {
	fmt.Println("------------------------------------------")
}

func print_report_table_footer() {
	fmt.Println("==========================================")
}

func main() {

	number_of_cpus := count_cpus()

	print_report_header(number_of_cpus)

	print_report_table_header()

	var number_of_tasks, duration int

	base_duration := measure_base_duration()

	print_report_table_header()

	for layer := 0; layer < 3; layer++ {
		for cpu := 0; cpu < number_of_cpus; cpu++ {
			number_of_tasks = 1 + cpu + layer*number_of_cpus
			duration = fulfil_observation(number_of_tasks)
			print_report_table_entry(number_of_tasks, base_duration, duration)
		}

		print_report_table_separator()
	}

	number_of_tasks = number_of_cpus * 10
	duration = fulfil_observation(number_of_tasks)
	print_report_table_entry(number_of_tasks, base_duration, duration)

	print_report_table_footer()

}
