package schedulor

import executorpkg "schedulor/executor"

// TaskHandlerFunc processes task payload decoded from queue JSON.
type TaskHandlerFunc = executorpkg.TaskHandlerFunc

// TaskHandler binds a task name to a Go handler function.
type TaskHandler = executorpkg.TaskHandler

// TaskExecutor is a pluggable execution strategy used by LqExecutor.
type TaskExecutor = executorpkg.TaskExecutor

// CodeTaskExecutor executes tasks via in-process Go handlers.
type CodeTaskExecutor = executorpkg.CodeTaskExecutor

// BashTaskExecutor executes commands from task payload fields.
type BashTaskExecutor = executorpkg.BashPayloadTaskExecutor

// BashFileTaskExecutor executes preconfigured commands loaded from file.
type BashFileTaskExecutor = executorpkg.BashFileTaskExecutor

// NewCodeTaskExecutor creates an executor for in-process handlers.
func NewCodeTaskExecutor(tasks []TaskHandler) *CodeTaskExecutor {
	return executorpkg.NewCodeTaskExecutor(tasks)
}

// NewBashTaskExecutor creates an executor that reads shell command from payload.
func NewBashTaskExecutor(taskNames []string, commandField string) *BashTaskExecutor {
	return executorpkg.NewBashPayloadTaskExecutor(taskNames, commandField)
}

// NewBashFileTaskExecutor creates an executor from in-memory task->command map.
func NewBashFileTaskExecutor(commands map[string]string) *BashFileTaskExecutor {
	return executorpkg.NewBashFileTaskExecutor(commands)
}

// NewBashFileTaskExecutorFromFile loads task->command mapping from JSON file.
func NewBashFileTaskExecutorFromFile(path string) (*BashFileTaskExecutor, error) {
	return executorpkg.NewBashFileTaskExecutorFromFile(path)
}
