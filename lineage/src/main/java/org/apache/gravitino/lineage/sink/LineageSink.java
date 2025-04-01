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

package org.apache.gravitino.lineage.sink;

import io.openlineage.server.OpenLineage;
import java.io.Closeable;
import java.util.Map;

/** The LineageSink interface defines a closable component responsible for sinking lineage event. */
public interface LineageSink extends Closeable {

  /**
   * Initializes the lineage sink with the provided configuration.
   *
   * @param configs A map representing the configuration for the sink.
   */
  default void initialize(Map<String, String> configs) {}

  /** Closes the lineage sink and releases associated resources. */
  @Override
  default void close() {}

  /**
   * Sinks the given lineage run event.
   *
   * @param event The lineage run event to be processed.
   */
  void sink(OpenLineage.RunEvent event);
}
