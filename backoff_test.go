package schedulor

import (
	"testing"
	"time"
)

func TestExponentialBackoff(t *testing.T) {
	type args struct {
		attempt     int
		maxAttempts int
	}
	tests := []struct {
		name string
		args args
		want time.Duration
	}{
		{
			"zero",
			args{0, 10},
			100 * time.Millisecond,
		},
		{
			"middle",
			args{5, 10},
			1600 * time.Millisecond,
		},
		{
			"overlimit",
			args{11, 10},
			10 * time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ExponentialBackoff(tt.args.attempt, tt.args.maxAttempts); got != tt.want {
				t.Errorf("ExponentialBackoff() = %v, want %v", got, tt.want)
			}
		})
	}
}
