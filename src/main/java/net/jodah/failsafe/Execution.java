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

/**
 * Tracks executions and determines when an execution can be performed for a {@link RetryPolicy}.
 * 
 * @author Jonathan Halterman
 */
public class Execution extends AbstractExecution {
  /**
   * Creates a new Execution.
   */
  public Execution() {
    super(null, new FailsafeConfig<Object, FailsafeConfig<Object, ?>>());
  }

  /**
   * Creates a new Execution for the {@code circuitBreaker}.
   * 
   * @throws NullPointerException if {@code circuitBreaker} is null
   * @deprecated Use {@link #Execution()} and {@link #with(CircuitBreaker)} instead
   */
  public Execution(CircuitBreaker circuitBreaker) {
    this();
    with(circuitBreaker);
  }

  /**
   * Creates a new Execution for the {@code retryPolicy}.
   * 
   * @throws NullPointerException if {@code retryPolicy} is null
   * @deprecated Use {@link #Execution()} and {@link #with(RetryPolicy)} instead
   */
  public Execution(RetryPolicy retryPolicy) {
    this();
    with(retryPolicy);
  }

  Execution(Callable<Object> callable, FailsafeConfig<Object, ?> config) {
    super(callable, config);
  }

  /**
   * Records an execution and returns true if a retry can be performed for the {@code result}, else returns false and
   * marks the execution as complete.
   * 
   * @throws IllegalStateException if the execution is already complete
   */
  public boolean canRetryFor(Object result) {
    return !complete(result, null, false);
  }

  /**
   * Records an execution and returns true if a retry can be performed for the {@code result} or {@code failure}, else
   * returns false and marks the execution as complete.
   * 
   * @throws IllegalStateException if the execution is already complete
   */
  public boolean canRetryFor(Object result, Throwable failure) {
    return !complete(result, failure, false);
  }

  /**
   * Records an execution and returns true if a retry can be performed for the {@code failure}, else returns false and
   * marks the execution as complete.
   * 
   * @throws NullPointerException if {@code failure} is null
   * @throws IllegalStateException if the execution is already complete
   */
  public boolean canRetryOn(Throwable failure) {
    Assert.notNull(failure, "failure");
    return !complete(null, failure, false);
  }

  /**
   * Records and completes the execution.
   * 
   * @throws IllegalStateException if the execution is already complete
   */
  public void complete() {
    complete(null, null, true);
  }

  /**
   * Records and attempts to complete the execution with the {@code result}. Returns true on success, else false if
   * completion failed and execution should be retried.
   *
   * @throws IllegalStateException if the execution is already complete
   */
  public boolean complete(Object result) {
    return complete(result, null, false);
  }

  /**
   * Records a failed execution and returns true if a retry can be performed for the {@code failure}, else returns false
   * and completes the execution.
   * 
   * <p>
   * Alias of {@link #canRetryOn(Throwable)}
   * 
   * @throws NullPointerException if {@code failure} is null
   * @throws IllegalStateException if the execution is already complete
   */
  public boolean recordFailure(Throwable failure) {
    return canRetryOn(failure);
  }

  /**
   * Configures the {@code circuitBreaker} to be used to control the rate of event execution.
   * 
   * @throws IllegalStateException if a {@link CircuitBreaker} has already been configured
   * @throws NullPointerException if {@code circuitBreaker} is null
   */
  public synchronized Execution with(CircuitBreaker circuitBreaker) {
    Assert.state(!config.policies.contains(CircuitBreaker.class),
        "A CircuitBreaker has already been configured for the Execution");
    config.with(circuitBreaker);
    executor.addPolicy(circuitBreaker);
    return this;
  }

  /**
   * Configures the {@code retryPolicy} to be used for retrying failed executions.
   * 
   * @throws IllegalStateException if a {@link RetryPolicy} has already been configured
   * @throws NullPointerException if {@code retryPolicy} is null
   */
  public synchronized Execution with(RetryPolicy retryPolicy) {
    Assert.state(!config.policies.contains(RetryPolicy.class),
        "A RetryPolicy has already been configured for the Execution");
    config.with(retryPolicy);
    executor.addPolicy(retryPolicy);
    return this;
  }

  /**
   * Records and attempts to complete the execution, returning true if complete else false.
   * 
   * @throws IllegalStateException if the execution is already complete
   */
  synchronized boolean complete(Object result, Throwable failure, boolean noResult) {
    PolicyResult<Object> pr = new PolicyResult<Object>(result, failure, noResult, false);
    executor.postExecute(pr);
    return completed;
  }
}
