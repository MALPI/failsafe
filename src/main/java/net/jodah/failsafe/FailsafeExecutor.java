/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */
package net.jodah.failsafe;

import static net.jodah.failsafe.internal.util.RandomDelay.randomDelay;
import static net.jodah.failsafe.internal.util.RandomDelay.randomDelayInRange;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import net.jodah.failsafe.RetryPolicy.DelayFunction;
import net.jodah.failsafe.function.CheckedBiFunction;
import net.jodah.failsafe.util.Duration;
import net.jodah.failsafe.util.concurrent.Scheduler;

/**
 * Executes a {@code callable} for a set of configured {@link FailsafePolicy} instances.
 * 
 * @author Jonathan Halterman
 */
class FailsafeExecutor<T> {
  private final AbstractExecution execution;
  private final FailsafeConfig<Object, ?> config;
  private final Callable<T> callable;
  private PolicyExecutor<T> head;

  // Mutable state
  volatile PolicyExecutor<T> lastExecuted;

  FailsafeExecutor(AbstractExecution execution, FailsafeConfig<Object, ?> config, Callable<T> callable) {
    this.execution = execution;
    this.config = config;
    this.callable = callable;

    PolicyExecutor<T> next = null;
    for (FailsafePolicy policy : config.policies)
      next = buildPolicyExecutor(policy, next);

    // Add Fallback as the last PolicyExecutor
    if (config.fallback != null)
      next = buildPolicyExecutor(config.fallback, next);

    head = next;
  }

  void addPolicy(FailsafePolicy policy) {
    head = buildPolicyExecutor(policy, head);
  }

  @SuppressWarnings("unchecked")
  private PolicyExecutor<T> buildPolicyExecutor(FailsafePolicy policy, PolicyExecutor<T> next) {
    PolicyExecutor<T> policyExecutor = (PolicyExecutor<T>) policy.toExecutor();
    policyExecutor.next = next;
    policyExecutor.pipeline = this;
    policyExecutor.execution = execution;
    return policyExecutor;
  }

  /**
   * The result of an individual {@link FailsafePolicy} execution.
   */
  static class PolicyResult<T> {
    final T result;
    final Throwable failure;
    final boolean noResult;
    final boolean hardFailure;

    PolicyResult(T result, Throwable failure) {
      this(result, failure, false, false);
    }

    PolicyResult(T result, Throwable failure, boolean noResult, boolean hardFailure) {
      this.result = result;
      this.failure = failure;
      this.noResult = noResult;
      this.hardFailure = hardFailure;
    }

    @Override
    public String toString() {
      return "PolicyResult [result=" + result + ", failure=" + failure + ", noResult=" + noResult + ", hardFailure="
          + hardFailure + "]";
    }
  }

  /**
   * Performs a synchronous execution.
   */
  PolicyResult<T> executeSync() {
    PolicyResult<T> result = head.executeSync();
    execution.success = head.success;
    return result;
  }

  /**
   * Handles a synchronous execution result.
   */
  void postExecute(PolicyResult<T> result) {
    handleResult(result, head);
    execution.success = head.success;
  }

  private void handleResult(PolicyResult<T> result, PolicyExecutor<T> policyExecutor) {
    // Traverse to the last executor
    while (policyExecutor.next != null)
      handleResult(result, policyExecutor.next);
    policyExecutor.postExecute(result);
  }

  /**
   * Continues an asynchronous execution from the last PolicyExecutor given the {@code result}.
   */
  PolicyResult<T> executeAsync(PolicyResult<T> pr, Scheduler scheduler, FailsafeFuture<T> future) {
    boolean shouldExecute = lastExecuted == null;
    pr = head.executeAsync(pr, scheduler, future, execution.waitNanos, shouldExecute);

    if (pr != null) {
      execution.completed = true;
      execution.success = pr.hardFailure ? false : head.success;
      config.handleComplete(pr.result, pr.failure, execution, execution.success);
      future.complete(pr.result, pr.failure, execution.success);
    }

    return pr;
  }

  /**
   * Executes a policy. Policies may contain pre or post execution behaviors.
   */
  abstract static class PolicyExecutor<T> {
    AbstractExecution execution;
    FailsafeExecutor<T> pipeline;
    PolicyExecutor<T> next;

    // Mutable state
    volatile boolean success;

    /**
     * Called before execution to return an alternative result or failure such as if execution is not allowed or needed.
     */
    PolicyResult<T> preExecute() {
      return null;
    }

    PolicyResult<T> executeSync() {
      PolicyResult<T> result = preExecute();
      if (result != null)
        return result;

      // Move right
      if (next != null) {
        result = next.executeSync();
      } else {
        // End of pipeline
        try {
          execution.before();
          result = new PolicyResult<T>(pipeline.callable.call(), null);
        } catch (Throwable t) {
          result = new PolicyResult<T>(null, t);
        }
      }

      return postExecute(result);
    }

    /**
     * Performs an async execution of the pipeline by first doing a pre-execute, calling the next executor, else
     * scheduling the pipeline's callable.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    PolicyResult<T> executeAsync(PolicyResult<T> result, Scheduler scheduler, FailsafeFuture<T> future, long waitNanos,
        boolean shouldExecute) {
      boolean shouldExecuteNext = shouldExecute || this.equals(pipeline.lastExecuted);
      pipeline.lastExecuted = this;

      if (shouldExecute) {
        result = preExecute();
        if (result != null)
          return result;
      }

      // Move right
      if (next != null) {
        result = next.executeAsync(result, scheduler, future, waitNanos, shouldExecuteNext);
      } else if (shouldExecute) {
        // End of pipeline
        try {
          if (!future.isDone() && !future.isCancelled())
            future.inject((Future) scheduler.schedule(pipeline.callable, waitNanos, TimeUnit.NANOSECONDS));
          return null;
        } catch (Throwable t) {
          success = false;
          return new PolicyResult<T>(null, t, false, true);
        }
      }

      // Handle the result coming back
      if (result != null && !result.hardFailure) {
        result = postExecute(result);

        // Back-propagate result
        if (next != null) {
          success = success && next.success;
        }
      }

      return result;
    }

    @SuppressWarnings("unchecked")
    PolicyResult<T> postExecute(PolicyResult<T> result) {
      execution.record((PolicyResult<Object>) result);
      return result;
    }

    /**
     * Returns a FailsafeCallable that handles failures according to the {@code retryPolicy}.
     */
    static <T> PolicyExecutor<T> of(RetryPolicy retryPolicy) {
      return new PolicyExecutor<T>() {
        // Mutable state
        private volatile boolean completed;
        private volatile boolean retriesExceeded;
        /** The fixed, backoff, random or computed delay time in nanoseconds. */
        private volatile long delayNanos = -1;
        /** The wait time, which is the delay time adjusted for jitter and max duration, in nanoseconds. */
        private volatile long waitNanos;

        @Override
        PolicyResult<T> executeSync() {
          while (true) {
            PolicyResult<T> result = super.executeSync();

            if (completed) {
              return result;
            } else {
              try {
                Thread.sleep(TimeUnit.NANOSECONDS.toMillis(waitNanos));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return new PolicyResult<T>(null, new FailsafeException(e));
              }

              pipeline.config.handleRetry(result.result, result.failure, execution);
            }
          }
        }

        @Override
        PolicyResult<T> executeAsync(PolicyResult<T> result, Scheduler scheduler, FailsafeFuture<T> future,
            long waitNanosOther, boolean shouldExecute) {
          while (true) {
            result = super.executeAsync(result, scheduler, future, waitNanos, shouldExecute);

            if (result == null || completed || result.hardFailure)
              return result;

            // Move right again
            shouldExecute = true;
          }
        }

        @Override
        @SuppressWarnings("unchecked")
        PolicyResult<T> postExecute(PolicyResult<T> pr) {
          pr = super.postExecute(pr);

          // Determine the computed delay
          long computedDelayNanos = -1;
          DelayFunction<Object, Throwable> delayFunction = (DelayFunction<Object, Throwable>) retryPolicy.getDelayFn();
          if (delayFunction != null && retryPolicy.canApplyDelayFn(pr.result, pr.failure)) {
            Duration computedDelay = delayFunction.computeDelay(pr.result, pr.failure, execution);
            if (computedDelay != null && computedDelay.toNanos() >= 0)
              computedDelayNanos = computedDelay.toNanos();
          }

          // Determine the non-computed delay
          if (computedDelayNanos == -1) {
            Duration delay = retryPolicy.getDelay();
            Duration delayMin = retryPolicy.getDelayMin();
            Duration delayMax = retryPolicy.getDelayMax();

            if (delayNanos == -1 && delay != null && !delay.equals(Duration.NONE))
              delayNanos = delay.toNanos();
            else if (delayMin != null && delayMax != null)
              delayNanos = randomDelayInRange(delayMin.toNanos(), delayMin.toNanos(), Math.random());

            // Adjust for backoff
            if (execution.executions != 1 && retryPolicy.getMaxDelay() != null)
              delayNanos = (long) Math.min(delayNanos * retryPolicy.getDelayFactor(),
                  retryPolicy.getMaxDelay().toNanos());
          }

          waitNanos = computedDelayNanos != -1 ? computedDelayNanos : delayNanos;

          // Adjust the wait time for jitter
          if (retryPolicy.getJitter() != null)
            waitNanos = randomDelay(waitNanos, retryPolicy.getJitter().toNanos(), Math.random());
          else if (retryPolicy.getJitterFactor() > 0.0)
            waitNanos = randomDelay(waitNanos, retryPolicy.getJitterFactor(), Math.random());

          // Adjust the wait time for max duration
          long elapsedNanos = execution.getElapsedTime().toNanos();
          if (retryPolicy.getMaxDuration() != null) {
            long maxRemainingWaitTime = retryPolicy.getMaxDuration().toNanos() - elapsedNanos;
            waitNanos = Math.min(waitNanos, maxRemainingWaitTime < 0 ? 0 : maxRemainingWaitTime);
            if (waitNanos < 0)
              waitNanos = 0;
          }

          // Copy to execution
          execution.waitNanos = waitNanos;

          // Calculate result
          boolean maxRetriesExceeded = retryPolicy.getMaxRetries() != -1
              && execution.executions > retryPolicy.getMaxRetries();
          boolean maxDurationExceeded = retryPolicy.getMaxDuration() != null
              && elapsedNanos > retryPolicy.getMaxDuration().toNanos();
          retriesExceeded = maxRetriesExceeded || maxDurationExceeded;
          boolean isAbortable = retryPolicy.canAbortFor(pr.result, pr.failure);
          boolean isRetryable = retryPolicy.canRetryFor(pr.result, pr.failure);
          boolean shouldRetry = !retriesExceeded && !pr.noResult && !isAbortable && retryPolicy.allowsRetries()
              && isRetryable;
          completed = isAbortable || !shouldRetry;
          success = completed && !isAbortable && !isRetryable && pr.failure == null;

          // Call listeners
          if (!success)
            pipeline.config.handleFailedAttempt(pr.result, pr.failure, execution);
          if (isAbortable)
            pipeline.config.handleAbort(pr.result, pr.failure, execution);
          else if (!success && retriesExceeded)
            pipeline.config.handleRetriesExceeded(pr.result, pr.failure, execution);

          return pr;
        }
      };

    }

    /**
     * Returns a FailsafeCallable that handles failures according to the {@code circuitBreaker}.
     */
    static <T> PolicyExecutor<T> of(CircuitBreaker circuitBreaker) {
      return new PolicyExecutor<T>() {
        @Override
        PolicyResult<T> preExecute() {
          boolean allowsExecution = circuitBreaker.allowsExecution();
          if (allowsExecution)
            circuitBreaker.before();
          return allowsExecution ? null : new PolicyResult<T>(null, new CircuitBreakerOpenException());
        }

        @Override
        PolicyResult<T> postExecute(PolicyResult<T> pr) {
          pr = super.postExecute(pr);

          long elapsedNanos = pipeline.execution.getElapsedTime().toNanos();
          Duration timeout = circuitBreaker.getTimeout();
          boolean timeoutExceeded = timeout != null && elapsedNanos >= timeout.toNanos();

          if (circuitBreaker.isFailure(pr.result, pr.failure) || timeoutExceeded) {
            circuitBreaker.recordFailure();
            success = false;
          } else {
            circuitBreaker.recordSuccess();
            success = true;
          }

          return pr;
        }
      };
    }

    /**
     * Returns an FailsafeCallable that handles failures using the {@code fallback} to convert a result.
     */
    static <T> PolicyExecutor<T> of(CheckedBiFunction<T, Throwable, T> fallback) {
      return new PolicyExecutor<T>() {
        @Override
        PolicyResult<T> postExecute(PolicyResult<T> pr) {
          pr = super.postExecute(pr);

          try {
            success = true;
            return new PolicyResult<T>(fallback.apply(pr.result, pr.failure), null);
          } catch (Exception e) {
            success = false;
            return new PolicyResult<T>(null, e);
          }
        }
      };
    }
  }
}
