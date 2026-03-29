#!/usr/bin/env python3
"""Circuit breaker pattern with half-open state and metrics."""
import sys, time

class CircuitBreaker:
    def __init__(self, failure_threshold=3, recovery_timeout=5, success_threshold=2):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        self.state = "closed"
        self.failures = 0
        self.successes = 0
        self.last_failure_time = 0
        self.total_calls = 0
        self.total_failures = 0
    def call(self, func, *args, **kwargs):
        self.total_calls += 1
        if self.state == "open":
            if time.time() - self.last_failure_time >= self.recovery_timeout:
                self.state = "half-open"
                self.successes = 0
            else:
                raise RuntimeError("Circuit is OPEN")
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise
    def _on_success(self):
        self.failures = 0
        if self.state == "half-open":
            self.successes += 1
            if self.successes >= self.success_threshold:
                self.state = "closed"
    def _on_failure(self):
        self.failures += 1
        self.total_failures += 1
        self.last_failure_time = time.time()
        if self.failures >= self.failure_threshold:
            self.state = "open"

def test():
    cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.1, success_threshold=1)
    assert cb.state == "closed"
    def fail(): raise ValueError("boom")
    def succeed(): return "ok"
    for _ in range(2):
        try: cb.call(fail)
        except ValueError: pass
    assert cb.state == "open"
    try:
        cb.call(succeed)
        assert False
    except RuntimeError:
        pass
    time.sleep(0.15)
    result = cb.call(succeed)
    assert result == "ok"
    assert cb.state == "closed"
    assert cb.total_calls >= 3
    print("  circuit_breaker2: ALL TESTS PASSED")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test": test()
    else: print("Circuit breaker pattern")
