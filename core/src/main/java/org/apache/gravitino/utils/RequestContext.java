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
<<<<<<< HEAD
 * <p>Currently tracks two pieces of state:
=======
 * <p>Currently tracks four pieces of state:
>>>>>>> 15259af5d ([#12872] fix(core): Capture and redact request query parameters in audit log entries (#12891))
 *
 * <ul>
 *   <li><b>remoteAddress</b> — the client IP resolved from {@code X-Forwarded-For} or {@link
 *       javax.servlet.http.HttpServletRequest#getRemoteAddr()}.
 *   <li><b>operationOutcome</b> — set by {@link org.apache.gravitino.listener.EventBus} when an
 *       operation-layer {@link org.apache.gravitino.listener.api.event.Event} or {@link
 *       org.apache.gravitino.listener.api.event.FailureEvent} is dispatched, so that {@code
<<<<<<< HEAD
 *       HttpAuditFilter} can skip emitting a redundant HTTP-level failure event for the same
 *       request.
=======
 *       HttpAuditFilter} can skip emitting a redundant HTTP-level fallback event for the same
 *       request. Exposed as two independent-looking flags ({@code operationFailureFired}/{@code
 *       operationSuccessFired}) for callers, but backed by a single tri-state value — normally a
 *       request's operation layer dispatches at most one terminal event (success or failure), never
 *       both, so there is nothing to track independently. That is an expectation on callers, not
 *       something this class enforces, so failure is treated as sticky: {@link
 *       #markOperationSuccessFired()} is a no-op once a failure has been recorded on this thread,
 *       so a later success dispatch can never erase it and cause a 4xx/5xx response to be
 *       double-logged as if no operation-layer failure had fired. {@code HttpAuditFilter} still
 *       produces both a success and a failure entry for the "operation succeeded but HTTP delivery
 *       failed" case, because that failure entry comes from the filter's own HTTP-status check, not
 *       from a second operation-layer flag.
 *   <li><b>auditExtras</b> — optional {@code customInfo} facts stashed by an inner dispatcher and
 *       consumed by the outer event dispatcher so one operation still produces one event.
 *   <li><b>requestQueryParams</b> — the current request's query parameters, captured raw (not
 *       redacted — see {@code AuditLogRedactor}) once per request by {@code RequestContextFilter}
 *       and read (non-destructively) by every {@link org.apache.gravitino.listener.api.event.Event}
 *       constructor.
>>>>>>> 15259af5d ([#12872] fix(core): Capture and redact request query parameters in audit log entries (#12891))
 * </ul>
 *
 * <p><b>Threading contract:</b> values must be set and cleared on the same (servlet) thread. Event
 * constructors read the value at construction time and store it as a field, so async listener
 * threads never access this class.
 */
public class RequestContext {

  /** The operation-layer event outcome recorded for the current request, if any. */
  private enum OperationOutcome {
    SUCCESS,
    FAILURE
  }

  private static final ThreadLocal<String> REMOTE_ADDRESS = new ThreadLocal<>();
<<<<<<< HEAD
  private static final ThreadLocal<Boolean> OPERATION_FAILURE_FIRED = new ThreadLocal<>();
=======
  private static final ThreadLocal<OperationOutcome> OPERATION_OUTCOME = new ThreadLocal<>();
  private static final ThreadLocal<Map<String, String>> AUDIT_EXTRAS = new ThreadLocal<>();
  private static final ThreadLocal<Map<String, String>> REQUEST_QUERY_PARAMS = new ThreadLocal<>();
>>>>>>> 15259af5d ([#12872] fix(core): Capture and redact request query parameters in audit log entries (#12891))

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
    OPERATION_OUTCOME.set(OperationOutcome.FAILURE);
  }

  /**
   * Returns {@code true} if an operation-layer failure event has already been dispatched on this
   * thread for the current request.
   *
   * @return {@code true} if the flag is set, {@code false} otherwise.
   */
  public static boolean isOperationFailureFired() {
    return OPERATION_OUTCOME.get() == OperationOutcome.FAILURE;
  }

  /**
   * Clears the recorded operation outcome for the current request thread. Must be called in a
   * {@code finally} block by {@code HttpAuditFilter} at the end of each request to prevent stale
   * values from leaking to the next request on the same Jetty thread.
   *
   * <p>Named after the failure flag for symmetry with {@link #resetOperationSuccessFired()}, but
   * clears the single underlying outcome either way — see the class-level doc.
   */
  public static void resetOperationFailureFired() {
    OPERATION_OUTCOME.remove();
  }

  /**
   * Marks that an operation-layer success {@code Event} has been dispatched for the current
   * request. Called by {@code EventBus.dispatchPostEvent()} for every success event that is not
   * itself an {@code HttpRequestEvent}.
   *
   * <p>A no-op if a failure was already recorded on this thread: the failure outcome is monotonic
   * (mirroring the pre-existing {@code operationFailureFired} boolean this replaced), so a success
   * event dispatched after a failure event for the same request — which should not normally happen,
   * but is not structurally prevented — can never erase it and cause {@code HttpAuditFilter} to
   * double-log a 4xx/5xx response as if no operation-layer failure had fired.
   */
  public static void markOperationSuccessFired() {
    if (OPERATION_OUTCOME.get() != OperationOutcome.FAILURE) {
      OPERATION_OUTCOME.set(OperationOutcome.SUCCESS);
    }
  }

  /**
   * Returns {@code true} if an operation-layer success event has already been dispatched on this
   * thread for the current request.
   *
   * @return {@code true} if the flag is set, {@code false} otherwise.
   */
  public static boolean isOperationSuccessFired() {
    return OPERATION_OUTCOME.get() == OperationOutcome.SUCCESS;
  }

  /**
   * Clears the recorded operation outcome for the current request thread. Must be called in a
   * {@code finally} block by {@code HttpAuditFilter} at the end of each request to prevent stale
   * values from leaking to the next request on the same Jetty thread.
   *
   * <p>Named after the success flag for symmetry with {@link #resetOperationFailureFired()}, but
   * clears the single underlying outcome either way — see the class-level doc.
   */
  public static void resetOperationSuccessFired() {
    OPERATION_OUTCOME.remove();
  }

  /**
<<<<<<< HEAD
=======
   * Stashes optional audit extras for the current request thread. An inner dispatcher calls this
   * before returning or throwing so the outer event dispatcher can attach the extras to the
   * existing table event.
   *
   * <p>A {@code null} or empty map clears any previously stashed extras.
   *
   * @param extras optional {@code customInfo} facts, or {@code null} to clear
   */
  public static void setAuditExtras(Map<String, String> extras) {
    if (extras == null || extras.isEmpty()) {
      AUDIT_EXTRAS.remove();
      return;
    }
    AUDIT_EXTRAS.set(ImmutableMap.copyOf(extras));
  }

  /**
   * Returns and clears audit extras stashed on this thread. The outer event dispatcher calls this
   * when constructing the terminal table event.
   *
   * @return an immutable extras map, or an empty map when none were stashed
   */
  public static Map<String, String> takeAuditExtras() {
    Map<String, String> extras = AUDIT_EXTRAS.get();
    AUDIT_EXTRAS.remove();
    return extras == null ? ImmutableMap.of() : extras;
  }

  /**
   * Stashes the current request's raw (not redacted) query parameters for the current request
   * thread. Called once per request by {@code RequestContextFilter}. Redaction happens later, at
   * audit-log format time, applied uniformly to the full merged {@code customInfo()} map — see
   * {@code AuditLogRedactor}'s class doc for why.
   *
   * <p>Unlike {@link #setAuditExtras(Map)}/{@link #takeAuditExtras()}, this is read
   * non-destructively: every {@link org.apache.gravitino.listener.api.event.Event} constructed on
   * this thread during the request reads the same snapshot.
   *
   * @param params the raw query parameters, or {@code null} to clear
   */
  public static void setRequestQueryParams(Map<String, String> params) {
    if (params == null || params.isEmpty()) {
      REQUEST_QUERY_PARAMS.remove();
      return;
    }
    REQUEST_QUERY_PARAMS.set(ImmutableMap.copyOf(params));
  }

  /**
   * Returns the current request's raw (not redacted) query parameters previously set on this
   * thread, without clearing them.
   *
   * @return an immutable query-parameter map, or an empty map when none were set
   */
  public static Map<String, String> getRequestQueryParams() {
    Map<String, String> params = REQUEST_QUERY_PARAMS.get();
    return params == null ? ImmutableMap.of() : params;
  }

  /**
>>>>>>> 15259af5d ([#12872] fix(core): Capture and redact request query parameters in audit log entries (#12891))
   * Removes all per-request bindings from the current thread. Must be called in a {@code finally}
   * block after the request completes to prevent thread-pool leaks.
   */
  public static void clear() {
    REMOTE_ADDRESS.remove();
<<<<<<< HEAD
    OPERATION_FAILURE_FIRED.remove();
=======
    OPERATION_OUTCOME.remove();
    AUDIT_EXTRAS.remove();
    REQUEST_QUERY_PARAMS.remove();
>>>>>>> 15259af5d ([#12872] fix(core): Capture and redact request query parameters in audit log entries (#12891))
  }
}
