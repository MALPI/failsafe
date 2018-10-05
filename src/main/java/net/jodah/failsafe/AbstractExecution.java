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
import java.util.concurrent.TimeUnit;

import net.jodah.failsafe.FailsafeExecutor.PolicyResult;
import net.jodah.failsafe.internal.util.Assert;
import net.jodah.failsafe.util.Duration;

abstract class AbstractExecution extends ExecutionContext {
  final FailsafeConfig<Object, ?> config;
  final FailsafeExecutor<Object> executor;

  // Internally mutable state
  long attemptStartTime;
  volatile Object lastResult;
  volatile Throwable lastFailure;

  // Externally mutable state
  /** The wait time in nanoseconds. */
  volatile long waitNanos;
  volatile boolean completed;
  volatile boolean success;

  /**
   * Creates a new Execution for the {@code config}.
   */
  AbstractExecution(Callable<Object> callable, FailsafeConfig<Object, ?> config) {
    super(new Duration(System.nanoTime(), TimeUnit.NANOSECONDS));
    this.config = config;
    this.executor = new FailsafeExecutor<Object>(this, config, callable);
  }

  /**
   * Returns the last failure that was recorded.
   */
  @SuppressWarnings("unchecked")
  public <T extends Throwable> T getLastFailure() {
    return (T) lastFailure;
  }

  /**
   * Returns the last result that was recorded.
   */
  @SuppressWarnings("unchecked")
  public <T> T getLastResult() {
    return (T) lastResult;
  }

  /**
   * Returns the time to wait before the next execution attempt. Returns {@code 0} if an execution has not yet occurred.
   */
  public Duration getWaitTime() {
    return new Duration(waitNanos, TimeUnit.NANOSECONDS);
  }

  /**
   * Returns whether the execution is complete.
   */
  public boolean isComplete() {
    return completed;
  }

  void before() {
    attemptStartTime = System.nanoTime();
  }

  /**
   * Records an execution attempt.
   * 
   * @throws IllegalStateException if the execution is already complete
   */
  void record(PolicyResult<Object> policyResult) {
    Assert.state(!completed, "Execution has already been completed");
    executions++;
    lastResult = policyResult.noResult ? null : policyResult.result;
    lastFailure = policyResult.failure;
  }
}
