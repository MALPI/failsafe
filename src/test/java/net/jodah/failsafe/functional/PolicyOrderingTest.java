package net.jodah.failsafe.functional;

import static org.testng.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;

import net.jodah.failsafe.CircuitBreaker;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;

public class PolicyOrderingTest {
  public void testRetryBeforeCircuitBreaker() {
    RetryPolicy rp = new RetryPolicy().withMaxRetries(1);
    CircuitBreaker cb = new CircuitBreaker();

    Failsafe.with(rp).with(cb).run(() -> {
      throw new Exception();
    });
  }

  public void testCircuitBreakerBeforeRetry() {
    AtomicInteger check = new AtomicInteger();
    CircuitBreaker cb = new CircuitBreaker().onOpen(() -> assertTrue(true));
    RetryPolicy rp = new RetryPolicy().withMaxRetries(1);

    Failsafe.with(cb).with(rp).run(() -> {
      throw new Exception();
    });
  }
}
