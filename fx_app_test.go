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

	app := NewFxApp(FxAppOptions{Components: []FxLifecycleComponent{component}})
	if app == nil {
		t.Fatalf("expected app instance")
	}
}

func TestNewFxAppPanicsOnEmptyComponents(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("expected panic")
		}
	}()
	_ = NewFxApp(FxAppOptions{})
}
