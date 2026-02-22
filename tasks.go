package schedulor

import executorpkg "schedulor/executor"

type TaskHandlerFunc = executorpkg.TaskHandlerFunc

type TaskHandler = executorpkg.TaskHandler

type TaskExecutor = executorpkg.TaskExecutor

type CodeTaskExecutor = executorpkg.CodeTaskExecutor

type BashTaskExecutor = executorpkg.BashPayloadTaskExecutor

type BashFileTaskExecutor = executorpkg.BashFileTaskExecutor

func NewCodeTaskExecutor(tasks []TaskHandler) *CodeTaskExecutor {
	return executorpkg.NewCodeTaskExecutor(tasks)
}

func NewBashTaskExecutor(taskNames []string, commandField string) *BashTaskExecutor {
	return executorpkg.NewBashPayloadTaskExecutor(taskNames, commandField)
}

func NewBashFileTaskExecutor(commands map[string]string) *BashFileTaskExecutor {
	return executorpkg.NewBashFileTaskExecutor(commands)
}

func NewBashFileTaskExecutorFromFile(path string) (*BashFileTaskExecutor, error) {
	return executorpkg.NewBashFileTaskExecutorFromFile(path)
}
