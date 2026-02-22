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

// BashTaskExecutor executes configured commands by task name or payload.
type BashTaskExecutor = executorpkg.BashTaskExecutor

// BashTaskCommand describes command configuration for one task.
type BashTaskCommand = executorpkg.TaskCommand

// NewCodeTaskExecutor creates an executor for in-process handlers.
func NewCodeTaskExecutor(tasks []TaskHandler) *CodeTaskExecutor {
	return executorpkg.NewCodeTaskExecutor(tasks)
}

// NewBashTaskExecutor creates an executor that reads shell command from payload.
func NewBashTaskExecutor(taskNames []string, commandField string) *BashTaskExecutor {
	return executorpkg.NewBashTaskExecutor(taskNames, commandField)
}

// NewBashTaskExecutorFromCommands creates an executor from in-memory task->command map.
func NewBashTaskExecutorFromCommands(commands map[string]string) *BashTaskExecutor {
	return executorpkg.NewBashTaskExecutorFromCommands(commands)
}

// NewBashTaskExecutorWithCommands creates an executor from extended command config.
func NewBashTaskExecutorWithCommands(commands map[string]BashTaskCommand) *BashTaskExecutor {
	return executorpkg.NewBashTaskExecutorWithCommands(commands)
}
