package schedulor

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/fx"

	"schedulor/execution"
)

// FxModule constructs the same App used by standalone applications and attaches it to Fx's
// lifecycle. The application must provide execution.Store to the container.
func FxModule(options ...Option) fx.Option {
	return fx.Options(
		fx.Provide(func(store execution.Store) (*App, error) { return New(store, options...) }),
		fx.Invoke(registerAppLifecycle),
	)
}

func registerAppLifecycle(lifecycle fx.Lifecycle, app *App) {
	var cancel context.CancelFunc
	done := make(chan error, 1)
	lifecycle.Append(fx.Hook{
		OnStart: func(context.Context) error {
			if cancel != nil {
				return errors.New("schedulor app already started")
			}
			ctx, stop := context.WithCancel(context.Background())
			cancel = stop
			go func() { done <- app.Run(ctx) }()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if cancel == nil {
				return nil
			}
			cancel()
			select {
			case err := <-done:
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return err
			case <-ctx.Done():
				return fmt.Errorf("stop schedulor app: %w", ctx.Err())
			}
		},
	})
}
