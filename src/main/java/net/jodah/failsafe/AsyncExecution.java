/*
 * Copyright 2016 the original author or authors.
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

import java.util.concurrent.Callable;

import net.jodah.failsafe.FailsafeExecutor.PolicyResult;
import net.jodah.failsafe.internal.util.Assert;
import net.jodah.failsafe.util.concurrent.Scheduler;

/**
 * Tracks asynchronous executions and allows retries to be scheduled according to a {@link RetryPolicy}.
 * 
 * @author Jonathan Halterman
 */
public final class AsyncExecution extends AbstractExecution {
  private final FailsafeFuture<Object> future;
  private final Scheduler scheduler;
  volatile boolean completeCalled;
  volatile boolean retryCalled;

  @SuppressWarnings("unchecked")
  <T> AsyncExecution(Callable<T> callable, Scheduler scheduler, FailsafeFuture<T> future,
      FailsafeConfig<Object, ?> config) {
    super((Callable<Object>) callable, config);
    this.scheduler = scheduler;
    this.future = (FailsafeFuture<Object>) future;
  }

  /**
   * Completes the execution and the associated {@code FutureResult}.
   *
   * @throws IllegalStateException if the execution is already complete
   */
  public void complete() {
    complete(null, null, true);
  }

  /**
   * Attempts to complete the execution and the associated {@code FutureResult} with the {@code result}. Returns true on
   * success, else false if completion failed and the execution should be retried via {@link #retry()}.
   *
   * @throws IllegalStateException if the execution is already complete
   */
  public boolean complete(Object result) {
    return complete(result, null, false);
  }

  /**
   * Attempts to complete the execution and the associated {@code FutureResult} with the {@code result} and
   * {@code failure}. Returns true on success, else false if completion failed and the execution should be retried via
   * {@link #retry()}.
   * <p>
   * Note: the execution may be completed even when the {@code failure} is not {@code null}, such as when the
   * RetryPolicy does not allow retries for the {@code failure}.
   *
   * @throws IllegalStateException if the execution is already complete
   */
  public boolean complete(Object result, Throwable failure) {
    return complete(result, failure, false);
  }

  /**
   * Records an execution and returns true if a retry has been scheduled for else returns returns false and completes
   * the execution and associated {@code FutureResult}.
   *
   * @throws IllegalStateException if a retry method has already been called or the execution is already complete
   */
  public boolean retry() {
    return retryFor(lastResult, lastFailure);
  }

  /**
   * Records an execution and returns true if a retry has been scheduled for the {@code result}, else returns false and
   * marks the execution and associated {@code FutureResult} as complete.
   *
   * @throws IllegalStateException if a retry method has already been called or the execution is already complete
   */
  public boolean retryFor(Object result) {
    return retryFor(result, null);
  }

  /**
   * Records an execution and returns true if a retry has been scheduled for the {@code result} or {@code failure}, else
   * returns false and marks the execution and associated {@code FutureResult} as complete.
   * 
   * @throws IllegalStateException if a retry method has already been called or the execution is already complete
   */
  public boolean retryFor(Object result, Throwable failure) {
    Assert.state(!retryCalled, "Retry has already been called");
    retryCalled = true;
    return complete(result, failure, false);
  }

  /**
   * Records an execution and returns true if a retry has been scheduled for the {@code failure}, else returns false and
   * marks the execution and associated {@code FutureResult} as complete.
   *
   * @throws NullPointerException if {@code failure} is null
   * @throws IllegalStateException if a retry method has already been called or the execution is already complete
   */
  public boolean retryOn(Throwable failure) {
    Assert.notNull(failure, "failure");
    return retryFor(null, failure);
  }

  // // @Override
  // PolicyResult<Object> preExecuteCheck() {
  // // result = super.preExecuteCheck();
  // // if (failure != null)
  // // future.complete(null, failure, false);
  // // return failure;
  // return null;
  // }

  /**
   * Prepares for an execution by performing a pre-execute check, recording the start time, resetting internal flags,
   * and calling retry listeners.
   */
  @Override
  void before() {
    // if (preExecuteCheck() != null)
    // return;

    if (completeCalled && config != null)
      config.handleRetry(lastResult, lastFailure, this);

    super.before();
    completeCalled = false;
    retryCalled = false;
  }

  /**
   * Attempts to complete the parent execution, calls failure handlers, and completes the future if needed.
   * 
   * @throws IllegalStateException if the execution is already complete
   */
  boolean complete(Object result, Throwable failure, boolean noResult) {
    synchronized (future) {
      if (!completeCalled) {
        completeCalled = true;
        PolicyResult<Object> pr = new PolicyResult<Object>(result, failure, noResult, false);
        executor.executeAsync(pr, scheduler, future);
      }

      return completed;
    }
  }
}
