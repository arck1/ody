package monitoring

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"schedulor/execution"
)

type ServiceSuite struct {
	suite.Suite
	ctx     context.Context
	store   *execution.MemoryStore
	service *Service
}

func TestServiceSuite(t *testing.T) { suite.Run(t, new(ServiceSuite)) }

func (s *ServiceSuite) SetupTest() {
	s.ctx = context.Background()
	s.store = execution.NewMemoryStore()
	var err error
	s.service, err = New(s.store)
	s.Require().NoError(err)
}

func (s *ServiceSuite) TestInspectAndRestartCompletedTask() {
	created, err := s.store.CreateExecution(s.ctx, execution.CreateExecution{TaskName: "report", Input: json.RawMessage(`{"day":1}`), MaxAttempts: 3})
	s.Require().NoError(err)
	claimed, err := s.store.Claim(s.ctx, "worker", []execution.TaskKey{{Name: "report", Version: 0}}, 1, time.Minute)
	s.Require().NoError(err)
	s.Require().Len(claimed, 1)
	s.Require().NoError(s.store.Succeed(s.ctx, created.ID, claimed[0].LeaseToken, json.RawMessage(`{"url":"done"}`)))

	details, err := s.service.Task(s.ctx, created.ID)
	s.Require().NoError(err)
	s.Equal(execution.StatusSucceeded, details.Execution.Status)
	s.Len(details.Events, 3)

	restarted, err := s.service.RestartTask(s.ctx, created.ID)
	s.Require().NoError(err)
	s.Equal(execution.StatusPending, restarted.Status)
	s.Zero(restarted.Attempt)
	s.Nil(restarted.Output)
	s.Nil(restarted.StartedAt)
	s.Nil(restarted.FinishedAt)

	details, err = s.service.Task(s.ctx, created.ID)
	s.Require().NoError(err)
	s.Equal(execution.EventRestarted, details.Events[len(details.Events)-1].Type)
	_, err = s.service.RestartTask(s.ctx, created.ID)
	s.ErrorIs(err, execution.ErrActive)
}

func (s *ServiceSuite) TestPipelineRestartRequiresCoordinator() {
	run, err := s.store.CreatePipelineRun(s.ctx, execution.CreatePipelineRun{PipelineName: "import"})
	s.Require().NoError(err)
	created, err := s.store.CreateExecution(s.ctx, execution.CreateExecution{TaskName: "fetch", PipelineRunID: &run.ID, NodeKey: "fetch"})
	s.Require().NoError(err)
	claimed, err := s.store.Claim(s.ctx, "worker", []execution.TaskKey{{Name: "fetch", Version: 0}}, 1, time.Minute)
	s.Require().NoError(err)
	s.Require().NoError(s.store.Fail(s.ctx, created.ID, claimed[0].LeaseToken, "network"))
	s.Require().NoError(s.store.SetPipelineRunStatus(s.ctx, run.ID, run.Revision, execution.RunFailed, "node fetch"))

	_, err = s.service.RestartTask(s.ctx, created.ID)
	s.ErrorIs(err, execution.ErrPipelineExecution)
	details, err := s.service.Pipeline(s.ctx, run.ID)
	s.Require().NoError(err)
	s.Equal(execution.RunFailed, details.Run.Status)
	s.Require().Len(details.Executions, 1)
	s.Equal(execution.StatusFailed, details.Executions[0].Status)
}
