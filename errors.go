package schedulor

import "fmt"

type UnknownTaskName struct {
	TaskId   int64
	TaskName string
}

func (e *UnknownTaskName) Error() string {
	return fmt.Sprintf("unknown task name %s: %d", e.TaskName, e.TaskId)
}
