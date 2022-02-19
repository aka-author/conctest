// * * ** *** ***** ******** ************* *********************
// Observing concurrent code execution on Go
//                                                   (\(\
//                                                  =(^.^)=
// * * ** *** ***** ******** ************* *********************

package main

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"time"
)

// Time

type TimeMs = int

func now_ms() TimeMs {
	return int(time.Now().UnixNano() / 1e6)
}

func duration_ms(initial_moment TimeMs) TimeMs {
	return now_ms() - initial_moment
}

// Spending time with fun

type Triplet = [3]float64

func random_item() float64 {
	return rand.Float64()
}

func random_triplet() Triplet {
	return Triplet{random_item(), random_item(), random_item()}
}

func get_next_triplet(triplet Triplet) Triplet {

	applicant := triplet[0] + triplet[1] - triplet[2]

	if math.Abs(applicant) <= 1.0 {
		return Triplet{triplet[1], triplet[2], applicant}
	} else {
		return Triplet{triplet[1], triplet[2], 1.0 / applicant}
	}
}

func approx_eq(f1, f2 float64) bool {
	return math.Abs(f1-f2) < 1e-14
}

func is_convergent(triplet, next_triplet Triplet) bool {
	return approx_eq(triplet[0], next_triplet[0]) &&
		approx_eq(triplet[1], next_triplet[1]) &&
		approx_eq(triplet[2], next_triplet[2])
}

func iterate(initial_triplet Triplet, n_cycles int) float64 {

	triplet := initial_triplet

	prokukarek := false

	for step := 0; step < n_cycles; step++ {

		next_triplet := get_next_triplet(triplet)

		if is_convergent(triplet, next_triplet) && !prokukarek {
			print_convergency(initial_triplet, step, triplet[2])
			prokukarek = true
		}

		triplet = next_triplet
	}

	return triplet[2]
}

func standard_task(n_cycles int) Task {
	start := now_ms()
	iterate(random_triplet(), n_cycles)
	return create_task(start, duration_ms(start))
}

// Managing observation outcomes

type Task struct {
	start    TimeMs
	duration TimeMs
}

func (t Task) get_start() TimeMs {
	return t.start
}

func (t *Task) recalc_start_relative(initial_moment TimeMs) {
	t.start = t.start - initial_moment
}

func (t Task) get_finish() TimeMs {
	return t.start + t.duration
}

func (t Task) get_duration() TimeMs {
	return t.duration
}

func create_task(start TimeMs, duration TimeMs) Task {
	return Task{start, duration}
}

type Observation struct {
	tasks              []Task
	concurrency_profit float64
}

func (o *Observation) register_task(task Task) {
	o.tasks = append(o.tasks, task)
}

func (o Observation) count_tasks() int {
	return int(len(o.tasks))
}

func (o Observation) get_earliest_start() TimeMs {

	earliest_start := o.tasks[0].get_start()

	for _, task := range o.tasks {
		if earliest_start > task.get_start() {
			earliest_start = task.get_start()
		}
	}

	return earliest_start
}

func (o Observation) get_latest_finish() TimeMs {

	latest_finish := o.tasks[0].get_finish()

	for _, task := range o.tasks {
		if latest_finish < task.get_finish() {
			latest_finish = task.get_finish()
		}
	}

	return latest_finish
}

func (o Observation) recalc_tasks_relative_earliest_start() {

	earliest_start := o.get_earliest_start()

	for task_idx := range o.tasks {
		o.tasks[task_idx].recalc_start_relative(earliest_start)
	}
}

func (o Observation) get_total_duration() TimeMs {
	return o.get_latest_finish() - o.get_earliest_start()
}

func (o Observation) sum_duration() TimeMs {

	var sum TimeMs = 0

	for _, task := range o.tasks {
		sum += task.get_duration()
	}

	return sum
}

func (o Observation) get_mean_task_duration() TimeMs {
	return o.sum_duration() / o.count_tasks()
}

func (o Observation) get_standard_deviation() TimeMs {

	var dispersion int = 0
	var deviation TimeMs

	mean_task_duration := o.get_mean_task_duration()

	for _, task := range o.tasks {
		deviation = mean_task_duration - task.get_duration()
		dispersion += int(deviation * deviation)
	}

	return 0 //int(math.Sqrt(float64(dispersion))) / (o.count_tasks() - 1)
}

func (o Observation) get_concurrency_profit() float64 {
	return o.concurrency_profit
}

func (o Observation) calc_concurrency_profit(task_duration_min TimeMs) float64 {

	serial_duration := int(task_duration_min) * o.count_tasks()

	o.concurrency_profit =
		1 - float64(o.get_total_duration())/float64(serial_duration)

	return o.concurrency_profit
}

func create_observation(tasks []Task) Observation {
	return Observation{tasks, 0.0}
}

type Report struct {
	observations []Observation
}

func (r Report) count_observations() int {
	return len(r.observations)
}

func (r Report) get_task_duration_min() TimeMs {
	return r.observations[0].get_total_duration()
}

func (r *Report) register_observation(obs Observation) {

	obs.recalc_tasks_relative_earliest_start()

	if r.count_observations() > 0 {
		obs.calc_concurrency_profit(r.get_task_duration_min())
	}

	r.observations = append(r.observations, obs)
}

func (r Report) get_observation(idx int) *Observation {
	return &(r.observations[idx])
}

func create_report() Report {
	return Report{[]Observation{}}
}

// Performing observations

func count_series(n_tasks, series_size int) int {

	n_series := n_tasks / series_size

	if series_size*n_series < n_tasks {
		n_series++
	}

	return n_series
}

func observe(n_tasks, n_cycles, series_size int) Observation {

	n_series := count_series(n_tasks, series_size)
	var count_tasks_total int = 0
	var count_tasks_series int = 0

	var tasks []Task

	for series_idx := 0; series_idx < n_series; series_idx++ {

		var syncler sync.WaitGroup

		// syncler.Add(1)

		for count_tasks_total < n_tasks && count_tasks_series < series_size {

			syncler.Add(1)

			go func() {
				tasks = append(tasks, standard_task(n_cycles))
				syncler.Done()
			}()

			count_tasks_series++
			count_tasks_total++
		}

		syncler.Wait()
	}

	return create_observation(tasks)
}

// Getting parameters of the current system

func count_cpus() int {
	return runtime.NumCPU()
}

func count_cycles_per_sec() int {

	var duration TimeMs = 0
	var n_cycles int = 1

	for duration < 1000 {
		n_cycles *= 10
		start := now_ms()
		iterate(random_triplet(), n_cycles)
		duration = duration_ms(start)
	}

	return 1000 * n_cycles / int(duration)
}

// Printing messages to a console

func print_salutation() {
	fmt.Printf("Testing concurrent code execution on Go\n\n")
}

func print_help() {
	fmt.Println("Commands and arguments")
	fmt.Println("Displaying system parameters:")
	fmt.Println("s")
	fmt.Println("Measuring profits of concurrency:")
	fmt.Println("p <Number of tasks> <Cycles in a task> <Tasks in a series> [Output file]")
}

func print_sysparams_header() {
	fmt.Println("====================================")
	fmt.Println("System parameter               Value")
	fmt.Println("====================================")
}

func print_cpus(n_cpus int) {
	fmt.Printf("CPUs available %21d\n", n_cpus)
}

func print_cycles_per_sec(cycles_per_sec int) {
	fmt.Printf("Cycles per second %18v\n", cycles_per_sec)
}

func print_sysparams_footer() {
	fmt.Println("====================================")
}

func print_profit_header() {
	fmt.Println("============================================================")
	fmt.Println("Tasks  Mean task duration  Std. dev.  Total duration  Profit")
	fmt.Println("============================================================")
}

func print_profit_entry(obs *Observation) {
	fmt.Printf("%5d %19d %10d %15d %6.0f%%\n",
		obs.count_tasks(),
		obs.get_mean_task_duration(),
		obs.get_standard_deviation(),
		obs.get_total_duration(),
		obs.get_concurrency_profit()*100.0)
}

func print_convergency(initial_triplet Triplet, step int, member float64) {
	fmt.Printf("The sequence has converged: %f, %f, and %f give %f since step %d.\n",
		initial_triplet[0],
		initial_triplet[1],
		initial_triplet[2],
		member,
		step)
}

func print_profit_separator() {
	fmt.Println("------------------------------------------------------------")
}

func print_profit_footer() {
	fmt.Println("============================================================")
}

// Formatting and saving a report

func format_observation_totals_section_header() string {
	return "Tasks,Mean task duration,Std. dev.,Total duration,Profit\n"
}

func format_observation_totals(obs *Observation) string {
	return fmt.Sprintf("%d, %d, %d, %d, %f%%\n",
		obs.count_tasks(),
		obs.get_mean_task_duration(),
		obs.get_standard_deviation(),
		obs.get_total_duration(),
		obs.get_concurrency_profit()*100.0)
}

func format_observation_totals_section_data(report *Report) string {

	formatted_data := ""

	for _, obs := range report.observations {
		formatted_data += format_observation_totals(&obs)
	}

	return formatted_data
}

func format_observation_totals_section(report *Report) string {
	return format_observation_totals_section_header() +
		format_observation_totals_section_data(report)
}

func format_task(n_tasks, task_idx int, task *Task) string {
	return fmt.Sprintf("%d,%d,%d,%d,%d\n",
		n_tasks,
		task_idx,
		task.get_start(),
		task.get_finish(),
		task.get_duration())
}

func format_tasks(obs *Observation) string {

	schedule_text := ""

	n_tasks := obs.count_tasks()
	task_idx := 1

	for _, task := range obs.tasks {
		schedule_text += format_task(n_tasks, task_idx, &task)
		task_idx++
	}

	return schedule_text
}

func format_observation_schedule_header() string {
	return "Tasks,Task,Started,Finished,Duration\n"
}

func format_observation_schedules_section(report *Report) string {

	section_text := format_observation_schedule_header()

	for _, obs := range report.observations {
		section_text += format_tasks(&obs)
	}

	return section_text
}

func format_report(report *Report) string {
	return format_observation_totals_section(report) +
		"\n" +
		format_observation_schedules_section(report)
}

func save_text(out_file_path string, text string) {

	if out_file_path != "" {

		out_file, err := os.Create(out_file_path)

		if err == nil {
			out_file.Write([]byte(text))
			out_file.Close()
		} else {
			panic(err)
		}
	}
}

// Performing observations

func test_sysparams() {
	print_sysparams_header()
	print_cpus(count_cpus())
	print_cycles_per_sec(count_cycles_per_sec())
	print_sysparams_footer()
}

func test_concurrency_profit(tasks_max, n_cycles, series_size int) Report {

	report := create_report()

	print_profit_header()

	for n_tasks := 1; n_tasks <= tasks_max; n_tasks++ {

		obs := observe(n_tasks, n_cycles, series_size)

		report.register_observation(obs)

		print_profit_entry(report.get_observation(n_tasks - 1))
		if n_tasks%count_cpus() == 0 && n_tasks != tasks_max {
			print_profit_separator()
		}
	}

	print_profit_footer()

	return report
}

// Accepting arguments

func validate_usize(s string) bool {
	r, _ := regexp.Compile(`^\d+$`)
	return r.Match([]byte(s))
}

func parse_int(s string) int {
	if validate_usize(s) {
		i, _ := strconv.Atoi(s)
		return int(i)
	} else {
		return 0
	}
}

type Command = int

const (
	CMD_Help = iota
	CMD_RequestSysParams
	CMD_MeasureConcurrencyProfit
)

const (
	ARG_IDX_COMMAND       = 1
	ARG_IDX_TASKS_MAX     = 2
	ARG_IDX_N_CYCLES      = 3
	ARG_IDX_SERIES_SIZE   = 4
	ARG_IDX_OUT_FILE_PATH = 5
)

type Args struct {
	command       Command
	tasks_max     int
	n_cycles      int
	series_size   int
	out_file_path string
}

func (a Args) get_command() Command {
	return a.command
}

func (a Args) get_tasks_max() int {
	return a.tasks_max
}

func (a Args) get_n_cycles() int {
	return a.n_cycles
}

func (a Args) get_series_size() int {
	return a.series_size
}

func (a Args) get_out_file_path() string {
	return a.out_file_path
}

func (a Args) parse_command(args []string) Command {

	var cmd Command = CMD_Help

	if len(args) > 1 {
		switch args[ARG_IDX_COMMAND] {
		case "s":
			cmd = CMD_RequestSysParams
		case "p":
			cmd = CMD_MeasureConcurrencyProfit
		default:
			cmd = CMD_Help
		}
	}

	return cmd
}

func (a Args) parse_tasks_max(args []string) int {
	return parse_int(args[ARG_IDX_TASKS_MAX])
}

func (a Args) parse_n_cycles(args []string) int {
	return parse_int(args[ARG_IDX_N_CYCLES])
}

func (a Args) parse_series_size(args []string) int {
	return parse_int(args[ARG_IDX_SERIES_SIZE])
}

func (a Args) parse_out_file_path(args []string) string {
	if len(args) == ARG_IDX_OUT_FILE_PATH+1 {
		return args[ARG_IDX_OUT_FILE_PATH]
	} else {
		return ""
	}
}

func (a *Args) parse(args []string) {

	if len(args) >= 1 {
		a.command = a.parse_command(args)
		if len(args) >= 4 {
			a.tasks_max = a.parse_tasks_max(args)
			a.n_cycles = a.parse_n_cycles(args)
			a.series_size = a.parse_series_size(args)
			a.out_file_path = a.parse_out_file_path(args)
		}
	}

	//return a
}

func (a Args) is_valid() bool {
	return a.get_tasks_max() > 0 &&
		a.get_n_cycles() > 0 &&
		a.get_series_size() > 0 &&
		a.get_series_size() <= a.get_tasks_max()
}

// Doing the job

func main() {

	print_salutation()

	var args Args

	args.parse(os.Args)

	switch args.get_command() {
	case CMD_Help:
		print_help()
	case CMD_RequestSysParams:
		test_sysparams()
	case CMD_MeasureConcurrencyProfit:
		if args.is_valid() {
			report := test_concurrency_profit(
				args.get_tasks_max(),
				args.get_n_cycles(),
				args.get_series_size())
			save_text(args.get_out_file_path(), format_report(&report))
		} else {
			print_help()
		}

	}
}

// Retrieving system parameters

/* func count_cpus() int {
	return runtime.NumCPU()
} */

// Spending time

/* func complex_task() float32 {

	var k float32

	for i := 0; i < 500000000; i++ {
		k += 2636625362.0 / 2763.0
	}

	fmt.Println(k)

	return k
} */

// Performing observations

/* func fulfil_observation(number_of_cpus int) int {

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
} */

// Printing a report

/* func print_report_header(number_of_cpus int) {
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

	"{:5} {:9} {:18.3} {:6}%", number_of_tasks, duration, k, profit

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

} */
