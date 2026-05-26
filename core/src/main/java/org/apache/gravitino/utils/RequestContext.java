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
 * Holds per-request context data (e.g. client IP) in a {@link ThreadLocal} so that event classes
 * constructed on the servlet thread can capture it without carrying a servlet dependency.
 *
 * <p><b>Threading contract:</b> values must be set and cleared on the same (servlet) thread. Event
 * constructors read the value at construction time and store it as a field, so async listener
 * threads never access this class.
 */
public class RequestContext {

  private static final ThreadLocal<String> REMOTE_ADDRESS = new ThreadLocal<>();

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
   * Removes the remote address binding from the current thread. Must be called in a {@code finally}
   * block after the request completes to prevent thread-pool leaks.
   */
  public static void clear() {
    REMOTE_ADDRESS.remove();
  }
}
