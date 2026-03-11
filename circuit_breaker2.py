#!/usr/bin/env python3
"""Circuit Breaker — Hystrix-style with half-open state and metrics."""
import time, threading, enum

class State(enum.Enum):
    CLOSED = "closed"; OPEN = "open"; HALF_OPEN = "half_open"

class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=30, half_open_max=3):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max = half_open_max
        self.state = State.CLOSED; self.failures = 0; self.successes = 0
        self.last_failure = 0; self.half_open_calls = 0
        self.lock = threading.Lock()
        self.metrics = {'total': 0, 'success': 0, 'failure': 0, 'rejected': 0, 'trips': 0}
    def call(self, fn, *args, **kwargs):
        with self.lock:
            self.metrics['total'] += 1
            if self.state == State.OPEN:
                if time.monotonic() - self.last_failure > self.recovery_timeout:
                    self.state = State.HALF_OPEN; self.half_open_calls = 0
                else:
                    self.metrics['rejected'] += 1
                    raise RuntimeError("Circuit OPEN")
            if self.state == State.HALF_OPEN and self.half_open_calls >= self.half_open_max:
                self.metrics['rejected'] += 1
                raise RuntimeError("Circuit HALF_OPEN limit")
        try:
            result = fn(*args, **kwargs)
            with self.lock:
                self.metrics['success'] += 1; self.successes += 1
                if self.state == State.HALF_OPEN:
                    self.half_open_calls += 1
                    if self.successes >= self.half_open_max:
                        self.state = State.CLOSED; self.failures = 0; self.successes = 0
                else: self.failures = 0
            return result
        except Exception as e:
            with self.lock:
                self.metrics['failure'] += 1; self.failures += 1
                self.last_failure = time.monotonic()
                if self.state == State.HALF_OPEN or self.failures >= self.failure_threshold:
                    self.state = State.OPEN; self.metrics['trips'] += 1
            raise

if __name__ == "__main__":
    cb = CircuitBreaker(failure_threshold=3, recovery_timeout=0.5)
    for i in range(5):
        try: cb.call(lambda: (_ for _ in ()).throw(Exception("fail")))
        except: pass
    print(f"State: {cb.state.value}, Metrics: {cb.metrics}")
    time.sleep(0.6)
    try: cb.call(lambda: "ok")
    except: pass
    print(f"After recovery: {cb.state.value}")
