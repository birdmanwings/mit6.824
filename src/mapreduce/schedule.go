package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	// wg to block the main goroutine
	var wg sync.WaitGroup

	for i := 0; i < ntasks; i++ {
		// set the wg number
		wg.Add(1)
		// set the args for call()
		var taskArgs DoTaskArgs
		taskArgs.JobName = jobName
		taskArgs.Phase = phase
		taskArgs.TaskNumber = i
		taskArgs.NumOtherPhase = n_other
		if phase == mapPhase {
			taskArgs.File = mapFiles[i]
		}
		// send the rpc
		go func() {
			defer wg.Done()
			// use loop to retry if rpc fail
			for {
				worker := <-registerChan
				if call(worker, "Worker.DoTask", &taskArgs, nil) == true {
					// Must use goroutine because registerChan is a unbuffered channel to avoid deadlock
					go func() { registerChan <- worker }()
					break
				}
			}
		}()

		// part3 code
		/*
			go func() {
					defer wg.Done()
					worker := <-registerChan
					if call(worker, "Worker.DoTask", &taskArgs, nil) != true {
						log.Fatal("Can't call")
					}
					// Must use goroutine because registerChan is a unbuffered channel to avoid deadlock
					go func() { registerChan <- worker }()
				}()
		*/
	}

	// Block the main goroutine
	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
