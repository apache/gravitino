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

package org.apache.gravitino.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * Provider of extra HTTP headers appended to every outgoing request. Implementations must be
 * thread-safe because {@link #getHeaders()} is called once per HTTP request and may be invoked
 * concurrently from multiple threads.
 *
 * <p>A typical use-case is forwarding per-request context (e.g. the originating user identity) that
 * is stored in a {@link ThreadLocal} and populated before each request is dispatched.
 *
 * @since 1.3.0
 */
public interface ExtraHeadersProvider extends Closeable {

  /**
   * Returns the extra headers to append to the current HTTP request. The returned map must not be
   * {@code null}; return an empty map when no headers should be added.
   *
   * @return a non-null, possibly empty map of header name → header value pairs
   */
  Map<String, String> getHeaders();

  /** Default no-op close implementation. Override if resources need to be released. */
  @Override
  default void close() throws IOException {}
}
