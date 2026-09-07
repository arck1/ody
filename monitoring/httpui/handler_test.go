package httpui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/arck1/ody/execution"
	"github.com/arck1/ody/monitoring"
)

type HandlerSuite struct {
	suite.Suite
	store   *execution.MemoryStore
	handler http.Handler
}

func TestHandlerSuite(t *testing.T) { suite.Run(t, new(HandlerSuite)) }

func (s *HandlerSuite) SetupTest() {
	s.store = execution.NewMemoryStore()
	service, err := monitoring.New(s.store)
	s.Require().NoError(err)
	handler, err := New(service, http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = response.Write([]byte("metric 1\n"))
	}))
	s.Require().NoError(err)
	s.handler = handler
}

func (s *HandlerSuite) TestListInspectAndRestartTask() {
	created, err := s.store.CreateExecution(context.Background(), execution.CreateExecution{TaskName: "email", Input: json.RawMessage(`{"to":"a@example.com"}`)})
	s.Require().NoError(err)
	s.Require().NoError(s.store.CancelExecution(context.Background(), created.ID, "test"))

	list := httptest.NewRecorder()
	s.handler.ServeHTTP(list, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/tasks?status=cancelled", nil))
	s.Equal(http.StatusOK, list.Code)
	s.Contains(list.Body.String(), created.ID.String())

	detail := httptest.NewRecorder()
	s.handler.ServeHTTP(detail, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/tasks/"+created.ID.String(), nil))
	s.Equal(http.StatusOK, detail.Code)
	s.Contains(detail.Body.String(), `"events"`)
	s.Contains(detail.Body.String(), `"to":"a@example.com"`)

	restart := httptest.NewRecorder()
	s.handler.ServeHTTP(restart, httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/api/tasks/"+created.ID.String()+"/restart", nil))
	s.Equal(http.StatusOK, restart.Code)
	updated, err := s.store.GetExecution(context.Background(), created.ID)
	s.Require().NoError(err)
	s.Equal(execution.StatusPending, updated.Status)
}

func (s *HandlerSuite) TestValidationAndMetrics() {
	invalid := httptest.NewRecorder()
	s.handler.ServeHTTP(invalid, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/api/tasks/not-a-uuid", nil))
	s.Equal(http.StatusBadRequest, invalid.Code)

	metrics := httptest.NewRecorder()
	s.handler.ServeHTTP(metrics, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/metrics", nil))
	s.Equal(http.StatusOK, metrics.Code)
	s.Equal("metric 1\n", metrics.Body.String())

	index := httptest.NewRecorder()
	s.handler.ServeHTTP(index, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil))
	s.Equal(http.StatusOK, index.Code)
	s.Contains(index.Body.String(), "Ody Operations")
}
