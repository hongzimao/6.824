package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.

	switch phase {
	case mapPhase:
		for i, f := range mr.files {
			
			args := new(DoTaskArgs)

			args.JobName = mr.jobName
			args.TaskNumber = i
			args.File = f 
			args.NumOtherPhase = mr.nReduce

			worker := <- mr.registerChannel
	
			ok := call(worker, "Worker.DoTask", args, new(struct{}))

			if ok == false {
				fmt.Printf("Worker %s DoTask error\n", worker)
			}

		}
	case reducePhase:
		for i := 0; i < mr.nReduce; i++ {

			args := new(DoTaskArgs)

			args.JobName = mr.jobName 
			args.TaskNumber = i
			args.NumOtherPhase = len(mr.files)

			worker := <- mr.registerChannel
			ok := call(worker, "Worker.DoTask", args, new(struct{}))

			if ok == false {
				fmt.Printf("Worker %s DoTask error\n", worker)
			}
		}
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}
