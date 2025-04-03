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

package org.apache.gravitino.lineage.source;

import java.io.Closeable;
import java.util.Map;
import org.apache.gravitino.lineage.LineageDispatcher;

/**
 * The LineageSource interface defines a closable data source for receiving and dispatching lineage
 * information.
 */
public interface LineageSource extends Closeable {

  /**
   * Initializes the data source with the given configurations and a lineage dispatcher.
   *
   * @param configs A map containing configuration information for the data source.
   * @param dispatcher A dispatcher used to distribute lineage event.
   */
  default void initialize(Map<String, String> configs, LineageDispatcher dispatcher) {}

  /** Closes the data source and releases related resources. */
  @Override
  default void close() {}
}
