// Package httpui provides an HTTP API and embedded operator dashboard.
package httpui

import (
	_ "embed"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"ody/execution"
	"ody/monitoring"
)

//go:embed index.html
var indexHTML []byte

type Handler struct {
	api monitoring.API
	mux *http.ServeMux
}

func New(api monitoring.API, metrics http.Handler) (*Handler, error) {
	if api == nil {
		return nil, errors.New("monitoring api is nil")
	}
	h := &Handler{api: api, mux: http.NewServeMux()}
	h.mux.HandleFunc("GET /", h.index)
	h.mux.HandleFunc("GET /api/tasks", h.tasks)
	h.mux.HandleFunc("GET /api/tasks/{id}", h.task)
	h.mux.HandleFunc("POST /api/tasks/{id}/restart", h.restart)
	h.mux.HandleFunc("POST /api/tasks/{id}/cancel", h.cancel)
	h.mux.HandleFunc("GET /api/pipelines", h.pipelines)
	h.mux.HandleFunc("GET /api/pipelines/{id}", h.pipeline)
	if metrics != nil {
		h.mux.Handle("GET /metrics", metrics)
	}
	return h, nil
}

func (h *Handler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	h.mux.ServeHTTP(response, request)
}

func (h *Handler) index(response http.ResponseWriter, request *http.Request) {
	if request.URL.Path != "/" {
		http.NotFound(response, request)
		return
	}
	response.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = response.Write(indexHTML)
}

func (h *Handler) tasks(response http.ResponseWriter, request *http.Request) {
	limit, ok := parseLimit(response, request)
	if !ok {
		return
	}
	cursor, ok := parseCursor(response, request)
	if !ok {
		return
	}
	items, err := h.api.ListTasks(request.Context(), execution.ListFilter{
		TaskName: request.URL.Query().Get("task_name"),
		Status:   execution.Status(request.URL.Query().Get("status")),
		Limit:    limit,
		Before:   cursor,
	})
	writeResult(response, items, err)
}

func (h *Handler) task(response http.ResponseWriter, request *http.Request) {
	id, ok := parseID(response, request)
	if !ok {
		return
	}
	item, err := h.api.Task(request.Context(), id)
	writeResult(response, item, err)
}

func (h *Handler) restart(response http.ResponseWriter, request *http.Request) {
	id, ok := parseID(response, request)
	if !ok {
		return
	}
	item, err := h.api.RestartTask(request.Context(), id)
	writeResult(response, item, err)
}

func (h *Handler) cancel(response http.ResponseWriter, request *http.Request) {
	id, ok := parseID(response, request)
	if !ok {
		return
	}
	reason := strings.TrimSpace(request.URL.Query().Get("reason"))
	if reason == "" {
		reason = "cancelled by operator"
	}
	err := h.api.CancelTask(request.Context(), id, reason)
	writeResult(response, map[string]bool{"cancelled": err == nil}, err)
}

func (h *Handler) pipelines(response http.ResponseWriter, request *http.Request) {
	limit, ok := parseLimit(response, request)
	if !ok {
		return
	}
	cursor, ok := parseCursor(response, request)
	if !ok {
		return
	}
	var statuses []execution.RunStatus
	for _, value := range request.URL.Query()["status"] {
		if value != "" {
			statuses = append(statuses, execution.RunStatus(value))
		}
	}
	items, err := h.api.ListPipelines(request.Context(), execution.RunFilter{Statuses: statuses, Limit: limit, Before: cursor})
	writeResult(response, items, err)
}

func parseLimit(response http.ResponseWriter, request *http.Request) (int, bool) {
	limit := 100
	if raw := request.URL.Query().Get("limit"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 1000 {
			writeError(response, http.StatusBadRequest, "limit must be between 1 and 1000")
			return 0, false
		}
		limit = value
	}
	return limit, true
}

func parseCursor(response http.ResponseWriter, request *http.Request) (*execution.Cursor, bool) {
	rawTime, rawID := request.URL.Query().Get("before_time"), request.URL.Query().Get("before_id")
	if rawTime == "" && rawID == "" {
		return nil, true
	}
	createdAt, timeErr := time.Parse(time.RFC3339Nano, rawTime)
	id, idErr := uuid.Parse(rawID)
	if timeErr != nil || idErr != nil {
		writeError(response, http.StatusBadRequest, "before_time and before_id must form a valid cursor")
		return nil, false
	}
	return &execution.Cursor{CreatedAt: createdAt, ID: id}, true
}

func (h *Handler) pipeline(response http.ResponseWriter, request *http.Request) {
	id, ok := parseID(response, request)
	if !ok {
		return
	}
	item, err := h.api.Pipeline(request.Context(), id)
	writeResult(response, item, err)
}

func parseID(response http.ResponseWriter, request *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(request.PathValue("id"))
	if err != nil {
		writeError(response, http.StatusBadRequest, "invalid id")
		return uuid.Nil, false
	}
	return id, true
}

func writeResult(response http.ResponseWriter, value any, err error) {
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, execution.ErrNotFound) {
			status = http.StatusNotFound
		} else if errors.Is(err, execution.ErrActive) {
			status = http.StatusConflict
		}
		writeError(response, status, err.Error())
		return
	}
	response.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(response).Encode(value)
}

func writeError(response http.ResponseWriter, status int, message string) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(map[string]string{"error": message})
}
