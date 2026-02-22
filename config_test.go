package schedulor

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jinzhu/copier"
)

func TestLoadSettingsFromEnvironment(t *testing.T) {
	originalDefaultSettings := LqSettings{}
	_ = copier.Copy(&originalDefaultSettings, &defaultSettings)
	defer func() {
		_ = copier.Copy(&defaultSettings, &originalDefaultSettings)
	}()
	os.Setenv("local_queue.postgres_queue.task_max_attempts", "999")
	os.Setenv("local_queue.scheduler.tasks_refresh_timeout", "45s")
	loadSettingsFromEnv()

	want := defaultSettings

	t.Run("load from env", func(t *testing.T) {
		if got := GetSettings(nil); !reflect.DeepEqual(got.TaskMaxAttempts, 999) {
			t.Errorf("GetSettings() = %v, want %v", got, want)
		}
		if got := GetSettings(nil); !reflect.DeepEqual(got.TasksRefreshTimeout, 45*time.Second) {
			t.Errorf("GetSettings() = %v, want %v", got, want)
		}
	})
}

func TestGetSettings(t *testing.T) {
	originalDefaultSettings := LqSettings{}
	_ = copier.Copy(&originalDefaultSettings, &defaultSettings)
	defer func() {
		_ = copier.Copy(&defaultSettings, &originalDefaultSettings)
	}()

	type args struct {
		options *LqSettings
	}
	tests := []struct {
		name string
		args args
		want LqSettings
	}{
		{
			name: "GetSettings with empty options",
			args: args{
				options: &LqSettings{},
			},
			want: defaultSettings,
		},
		{
			name: "GetSettings with nil",
			args: args{
				options: nil,
			},
			want: defaultSettings,
		},
		{
			name: "GetSettings with nil",
			args: args{
				options: &LqSettings{
					LqPostgresQueueOptions: &LqPostgresQueueOptions{
						TaskMaxAttempts: 99,
						TaskVisibility:  66 * time.Second,
					},
				},
			},
			want: LqSettings{
				LqPostgresQueueOptions: &LqPostgresQueueOptions{
					TaskMaxAttempts: 99,
					TaskVisibility:  66 * time.Second,
				},
				LqLeaderElectorOptions: defaultSettings.LqLeaderElectorOptions,
				LqSchedulerOptions:     defaultSettings.LqSchedulerOptions,
				LqExecutorOptions:      defaultSettings.LqExecutorOptions,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetSettings(tt.args.options); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetSettings() = %v, want %v", got, tt.want)
			}
		})
	}
}
