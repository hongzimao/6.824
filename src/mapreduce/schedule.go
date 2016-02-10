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
			
			dotask_args := new(DoTaskArgs)
			dotask_args.Phase = mapPhase;
			dotask_args.JobName = mr.jobName
			dotask_args.TaskNumber = i
			dotask_args.File = f 
			dotask_args.NumOtherPhase = mr.nReduce

			worker := <- mr.registerChannel

			ok := call(worker, "Worker.DoTask", dotask_args, new(struct{}))

			for ok == false {
				fmt.Printf("Worker %s DoTask error\n", worker)

				worker = <- mr.registerChannel // connect to another worker
				ok = call(worker, "Worker.DoTask", dotask_args, new(struct{})) // do the task again
			}		

			go func(){ mr.registerChannel <- worker }()
		}

	case reducePhase:
		for i := 0; i < mr.nReduce; i++ {

			dotask_args := new(DoTaskArgs)

			dotask_args.Phase = reducePhase;
			dotask_args.JobName = mr.jobName 
			dotask_args.TaskNumber = i
			dotask_args.NumOtherPhase = len(mr.files)

			worker := <- mr.registerChannel

			ok := call(worker, "Worker.DoTask", dotask_args, new(struct{}))

			for ok == false {
				fmt.Printf("Worker %s DoTask error\n", worker)

				worker = <- mr.registerChannel // connect to another worker
				ok = call(worker, "Worker.DoTask", dotask_args, new(struct{})) // do the task again
			}

			go func(){ mr.registerChannel <- worker }()
		}
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}
