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

/**
 * Holds per-request context data in a {@link ThreadLocal} so that event classes constructed on the
 * servlet thread can capture it without carrying a servlet dependency.
 *
 * <p>Currently tracks two pieces of state:
 *
 * <ul>
 *   <li><b>remoteAddress</b> — the client IP resolved from {@code X-Forwarded-For} or {@link
 *       javax.servlet.http.HttpServletRequest#getRemoteAddr()}.
 *   <li><b>operationFailureFired</b> — set to {@code true} by {@link
 *       org.apache.gravitino.listener.EventBus} when an operation-layer {@link
 *       org.apache.gravitino.listener.api.event.FailureEvent} is dispatched, so that {@code
 *       HttpAuditFilter} can skip emitting a redundant HTTP-level failure event for the same
 *       request.
 * </ul>
 *
 * <p><b>Threading contract:</b> values must be set and cleared on the same (servlet) thread. Event
 * constructors read the value at construction time and store it as a field, so async listener
 * threads never access this class.
 */
public class RequestContext {

  private static final ThreadLocal<String> REMOTE_ADDRESS = new ThreadLocal<>();
  private static final ThreadLocal<Boolean> OPERATION_FAILURE_FIRED = new ThreadLocal<>();

  private RequestContext() {}

  /**
   * Sets the client remote address for the current request thread.
   *
   * @param remoteAddress the client IP address (or the first entry of {@code X-Forwarded-For}).
   */
  public static void setRemoteAddress(String remoteAddress) {
    REMOTE_ADDRESS.set(remoteAddress);
  }

  /**
   * Returns the client remote address previously set on this thread, or {@code null} if none was
   * set.
   *
   * @return the client remote address, or {@code null}.
   */
  public static String getRemoteAddress() {
    return REMOTE_ADDRESS.get();
  }

  /**
   * Marks that an operation-layer {@code FailureEvent} has been dispatched for the current request.
   * Called by {@code EventBus.dispatchFailureEvent()} for every failure event that is not itself an
   * {@code HttpRequestFailureEvent}.
   */
  public static void markOperationFailureFired() {
    OPERATION_FAILURE_FIRED.set(Boolean.TRUE);
  }

  /**
   * Returns {@code true} if an operation-layer failure event has already been dispatched on this
   * thread for the current request.
   *
   * @return {@code true} if the flag is set, {@code false} otherwise.
   */
  public static boolean isOperationFailureFired() {
    return Boolean.TRUE.equals(OPERATION_FAILURE_FIRED.get());
  }

  /**
   * Clears the operation-failure flag for the current request thread. Must be called in a {@code
   * finally} block by {@code HttpAuditFilter} at the end of each request to prevent stale values
   * from leaking to the next request on the same Jetty thread.
   */
  public static void resetOperationFailureFired() {
    OPERATION_FAILURE_FIRED.remove();
  }

  /**
   * Removes all per-request bindings from the current thread. Must be called in a {@code finally}
   * block after the request completes to prevent thread-pool leaks.
   */
  public static void clear() {
    REMOTE_ADDRESS.remove();
    OPERATION_FAILURE_FIRED.remove();
  }
}
