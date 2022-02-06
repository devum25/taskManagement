package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/devum99/alivecor/models"
)

type Executor struct {
	Status     string
	Dispatcher *Dispatcher
	Queue      *models.Queue
	Quit       chan bool
}

type Cleaner struct {
	Dispatcher *Dispatcher
}

type TaskExecutable func() error

type DispatchStatus struct {
	Type   string
	ID     int
	Task   *models.Task
	Status string
}

type Dispatcher struct {
	taskCounter    int                  // internal counter for number of tasks
	taskQueue      chan *models.Task    // channel of jobs submitted by main()
	dispatchStatus chan *DispatchStatus // channel for job/worker status reports
	cleanerStatus  chan *DispatchStatus
}

func NewCleaner(dispactcher *Dispatcher) *Cleaner {
	c := &Cleaner{
		Dispatcher: dispactcher,
	}

	return c
}

// cleaner monitors each task on completion and add/log based on status

func (c *Cleaner) Start() {
	go func() {
		for {
			select {
			case ds := <-c.Dispatcher.cleanerStatus:
				fmt.Printf("Cleaning task [%d] with status [%v]", ds.ID, ds.Status)
				if ds.Status == "retry" {
					c.Dispatcher.AddTask(ds.Task.Id, ds.Task.F)
				} else if ds.Status == "timeout" {
					c.Dispatcher.LogTimeOut(ds.Task.Id)
				}

			}
		}
	}()
}

func NewExecutor(dispatcher *Dispatcher, queue *models.Queue) *Executor {
	e := &Executor{
		Dispatcher: dispatcher,
		Queue:      queue,
	}

	return e
}

// Executor will keep on running and fetch first item from queue to execute
func (e *Executor) Start(start <-chan bool) {
	e.Status = "START"
	go func() {
		for {
			select {
			case <-start:
				{
					for e.Dispatcher.taskCounter > 0 {

						task := e.Queue.Dequeue()
						if task != nil {
							fmt.Printf("Worker[%d] executing job[%v].\n", task.Id, task.Status)
							if (time.Now().Minute() - task.CreationTime.Minute()) > 5 {
								e.Dispatcher.dispatchStatus <- &DispatchStatus{Type: "executor", ID: task.Id, Status: "timeout", Task: task}
							} else {
								task.F()
								e.Dispatcher.dispatchStatus <- &DispatchStatus{Type: "executor", ID: task.Id, Status: "completed", Task: task}
							}
						}

					}
				}

			}
		}

	}()
}

func CreateNewDispatcher() *Dispatcher {
	d := &Dispatcher{
		taskCounter:    0,
		taskQueue:      make(chan *models.Task),
		dispatchStatus: make(chan *DispatchStatus),
		cleanerStatus:  make(chan *DispatchStatus),
	}
	return d
}

func (d *Dispatcher) Start() {
	queue := &models.Queue{}
	executor := NewExecutor(d, queue)

	startExecutor := make(chan bool)
	//doneExecutor := make(chan bool)

	cleaner := NewCleaner(d)
	cleaner.Start()

	// add task to queue and send it to cleaner on completion
	go func() {
		for {
			select {
			case task := <-d.taskQueue:
				fmt.Printf("Got a task in the queue to dispatch: %d\n", task.Id)
				queue.Enqueue(task)
				if executor.Status != "START" {
					executor.Status = "START"
					executor.Start(startExecutor)
					startExecutor <- true
				}
			case ds := <-d.dispatchStatus:
				fmt.Printf("Got a dispatch status:\n\tType[%s] - ID[%d] - Status[%s]\n", ds.Type, ds.ID, ds.Status)
				if ds.Type == "executor" {
					if ds.Status == "completed" {
						d.taskCounter--
						fmt.Printf("task Counter reduced to: %d\n", d.taskCounter)
						d.cleanerStatus <- &DispatchStatus{Type: "dispatcher", ID: ds.ID, Status: ds.Status, Task: ds.Task}
					} else if ds.Status == "timeout" {
						d.cleanerStatus <- &DispatchStatus{Type: "dispatcher", ID: ds.ID, Status: "timeout", Task: ds.Task}
					} else if ds.Status == "fail" {
						d.cleanerStatus <- &DispatchStatus{Type: "dispatcher", ID: ds.ID, Status: "retry", Task: ds.Task}
					}
				}
			}
		}
	}()
}

// Add task to queue with dispatcher
func (d *Dispatcher) AddTask(id int, je TaskExecutable) {
	t := &models.Task{Id: id, CreationTime: time.Now(), IsCompleted: false, Status: "untouched", F: je}
	go func() { d.taskQueue <- t }()
	d.taskCounter++
	fmt.Printf("task Counter is now: %d\n", d.taskCounter)
}

// Log timeout when creation time of task is more than 5 minutes
func (d *Dispatcher) LogTimeOut(id int) {
	d.taskCounter--
	fmt.Printf("task [%d] is timedout:", id)
}

//  Close dispatcher when there is no task to execute
func (d *Dispatcher) Finished() bool {
	if d.taskCounter < 1 {
		return true
	} else {
		return false
	}
}

func main() {

	dispatcher := CreateNewDispatcher()
	dispatcher.Start()
	go func() {
		for i := 0; i < 10; i++ {
			x := rand.Intn(10)
			task := func() error {

				fmt.Printf("task [%d]: performing work.\n", i)
				time.Sleep(time.Duration(x) * time.Second)
				fmt.Printf("Work done.\n")
				return nil
			}

			dispatcher.AddTask(i, task)
		}
	}()

	time.Sleep(5 * time.Second)
	for {
		if dispatcher.Finished() {
			fmt.Printf("All jobs finished.\n")
			break
		}
	}
}
