# schedulor

## Bash File Executor

`LqExecutor` поддерживает режим выполнения задач через bash-команды из JSON-файла.

Переменные окружения:

- `local_queue.executor.mode=bash_file`
- `local_queue.executor.bash_commands_file=/absolute/path/to/commands.json`

Пример `commands.json`:

```json
{
  "tasks": {
    "cleanup_tmp": "rm -rf /tmp/app-cache/*",
    "sync_reports": "/app/scripts/sync_reports.sh"
  },
  "list": [
    {
      "task_name": "health_ping",
      "command": "curl -fsS http://localhost:8080/health"
    }
  ]
}
```

При `mode=code` (по умолчанию) используются `TaskHandler` функции из Go-кода.
