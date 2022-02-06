package models

import "time"

type Task struct {
	Id           int
	IsCompleted  bool
	Status       string    // untouched, completed, failed, timeout
	CreationTime time.Time // when was the task created
	TaskData     string    // field containing data about the task
	F            func() error
}

type Queue struct {
	tasks []*Task
}

//Enqueue  adds a value at the end
func (q *Queue) Enqueue(t *Task) {
	q.tasks = append(q.tasks, t)
}

//Dequeue remove first element
func (q *Queue) Dequeue() *Task {
	if len(q.tasks) > 0 {
		toRemove := q.tasks[0]
		q.tasks = q.tasks[1:]
		return toRemove
	}

	return nil
}
