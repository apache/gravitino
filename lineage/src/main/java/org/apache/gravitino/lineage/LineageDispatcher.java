/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.lineage;

import io.openlineage.server.OpenLineage.RunEvent;
import java.io.Closeable;

/**
 * Dispatches lineage events to configured sinks after processing. Implementations should handle
 * initialization, event processing, and resource cleanup through {@link Closeable}.
 *
 * <p>Typical lifecycles:
 *
 * <ol>
 *   <li>{@link #initialize(LineageConfig)} with required configurations
 *   <li>Repeated calls to {@link #dispatchLineageEvent(io.openlineage.server.OpenLineage.RunEvent)}
 *   <li>{@link #close()} for resource cleanup
 * </ol>
 */
public interface LineageDispatcher extends Closeable {

  /**
   * Initializes the dispatcher with configuration. Must be called before event dispatching.
   *
   * @param lineageConfig configuration for lineage source, processor and sinks.
   */
  void initialize(LineageConfig lineageConfig);

  /**
   * Dispatches a lineage run event to the configured sink after processing.
   *
   * <p>Callers should implement appropriate retry/logging mechanisms for rejected events to prevent
   * system overload.
   *
   * @param runEvent The OpenLineage run event to be processed and dispatched. Must not be null.
   * @return {@code true} if the event was successfully processed and dispatched to the sinks,
   *     {@code false} if the event was rejected due to the overload of lineage sinks.
   */
  boolean dispatchLineageEvent(RunEvent runEvent);
}
