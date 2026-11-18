/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** Runs actions on separate threads that are released together by a barrier. */
public final class RaceTestUtils {

  private static final long TIMEOUT_SECONDS = 30;

  private RaceTestUtils() {}

  /**
   * Runs {@code action} on {@code threads} threads at the same time.
   *
   * @param threads the number of threads
   * @param action the action every thread runs
   * @return each thread's outcome, see {@link #runTogether(List)}
   * @throws Exception if a thread does not finish in time
   */
  public static List<Object> runTogether(int threads, Callable<?> action) throws Exception {
    return runTogether(Collections.nCopies(threads, action));
  }

  /**
   * Runs every action on its own thread, released at the same time by a barrier.
   *
   * @param actions the actions to run
   * @return the outcome of each action in input order: its return value, or the exception it threw
   * @throws Exception if an action does not finish in time
   */
  public static List<Object> runTogether(List<? extends Callable<?>> actions) throws Exception {
    CyclicBarrier barrier = new CyclicBarrier(actions.size());
    ExecutorService executor = Executors.newFixedThreadPool(actions.size());
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (Callable<?> action : actions) {
        futures.add(
            executor.submit(
                () -> {
                  barrier.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                  return action.call();
                }));
      }

      List<Object> outcomes = new ArrayList<>();
      for (Future<?> future : futures) {
        try {
          outcomes.add(future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        } catch (ExecutionException e) {
          outcomes.add(e.getCause());
        }
      }
      return outcomes;
    } finally {
      executor.shutdownNow();
    }
  }
}
