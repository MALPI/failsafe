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

import net.jodah.failsafe.FailsafeExecutor.PolicyExecutor;
import net.jodah.failsafe.function.CheckedBiFunction;

/**
 * Defines a policy for handling execution failures.
 * 
 * @author Jonathan Halterman
 */
public abstract class FailsafePolicy {
  abstract PolicyExecutor<?> toExecutor();

  /**
   * Returns a FailsafePolicy that handles failures using the {@code fallback}.
   */
  static <T> FailsafePolicy of(CheckedBiFunction<T, Throwable, T> fallback) {
    return new FailsafePolicy() {
      @Override
      PolicyExecutor<T> toExecutor() {
        return (PolicyExecutor<T>) PolicyExecutor.<T> of(fallback);
      }
    };
  }
}
