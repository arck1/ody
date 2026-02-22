package schedulor

import (
	"math"
	"time"
)

const (
	backoffBase     = float64(100 * time.Millisecond)
	backoffMaxDelay = float64(10 * time.Second)
)

// ExponentialBackoff возвращает время ожидания перед retry.
// attempt — текущий номер попытки начиная с 1
// maxAttempts — максимальное количество попыток (для ограничения роста задержки)
func ExponentialBackoff(attempt, maxAttempts int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > maxAttempts {
		// Используем задержку последней попытки, если выходим за предел
		attempt = maxAttempts
	}

	// экспоненциальный рост: base * 2^(attempt-1)
	delay := backoffBase * math.Pow(2, float64(attempt-1))

	// ограничиваем задержку максимумом
	if delay > backoffMaxDelay {
		delay = backoffMaxDelay
	}

	return time.Duration(delay)
}
