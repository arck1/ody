package schedulor

import (
	"context"
	"sync/atomic"
	"testing"

	"go.uber.org/fx"
)

type testLifecycleComponent struct {
	started *atomic.Bool
}

func (c *testLifecycleComponent) Init(lifecycle fx.Lifecycle) {
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			c.started.Store(true)
			return nil
		},
	})
}

func TestNewFxAppBuildsWithComponents(t *testing.T) {
	started := &atomic.Bool{}
	component := &testLifecycleComponent{started: started}

	app, err := NewFxApp(FxAppOptions{Components: []FxLifecycleComponent{component}})
	if err != nil {
		t.Fatalf("NewFxApp error: %v", err)
	}
	if app == nil {
		t.Fatalf("expected app instance")
	}
}

func TestNewFxAppReturnsErrorOnEmptyComponents(t *testing.T) {
	_, err := NewFxApp(FxAppOptions{})
	if err == nil {
		t.Fatalf("expected error")
	}
}
