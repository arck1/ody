package schedulor

import (
	"fmt"

	"go.uber.org/fx"
)

// FxLifecycleComponent is minimal contract for fx-managed runtime components.
type FxLifecycleComponent interface {
	// Init registers start/stop hooks in provided fx lifecycle.
	Init(lifecycle fx.Lifecycle)
}

// FxAppOptions configures modular fx app assembly.
type FxAppOptions struct {
	// Components are initialized and attached to lifecycle in the given order.
	Components []FxLifecycleComponent
	// Options allows passing extra fx options (logging, modules, decorators, etc).
	Options []fx.Option
}

// NewFxApp creates ready-to-run fx app from provided lifecycle components.
func NewFxApp(options FxAppOptions) (*fx.App, error) {
	if len(options.Components) == 0 {
		return nil, fmt.Errorf("fx app requires at least one lifecycle component")
	}

	fxOptions := make([]fx.Option, 0, len(options.Options)+2)
	fxOptions = append(fxOptions,
		fx.Supply(options.Components),
		fx.Invoke(registerLifecycleComponents),
	)
	fxOptions = append(fxOptions, options.Options...)

	return fx.New(fxOptions...), nil
}

func registerLifecycleComponents(lifecycle fx.Lifecycle, components []FxLifecycleComponent) {
	for _, component := range components {
		component.Init(lifecycle)
	}
}
