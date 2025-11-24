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
package org.apache.gravitino.lance.common.ops;

import org.apache.gravitino.lance.common.config.LanceConfig;

public abstract class NamespaceWrapper {

  public static final String NAMESPACE_DELIMITER_DEFAULT = "$";
  private final LanceConfig config;

  private volatile boolean initialized = false;
  private LanceNamespaceOperations namespaceOps;
  private LanceTableOperations tableOps;

  public NamespaceWrapper(LanceConfig config) {
    this.config = config;
  }

  protected abstract void initialize();

  protected abstract LanceNamespaceOperations newNamespaceOps();

  protected abstract LanceTableOperations newTableOps();

  public abstract void close() throws Exception;

  public LanceNamespaceOperations asNamespaceOps() {
    // lazy initialize the operations because it may block the startup
    initIfNeeded();
    return namespaceOps;
  }

  public LanceTableOperations asTableOps() {
    // lazy initialize the operations because it may block the startup
    initIfNeeded();
    return tableOps;
  }

  public LanceConfig config() {
    return config;
  }

  private void initAll() {
    initialize();
    namespaceOps = newNamespaceOps();
    tableOps = newTableOps();
    initialized = true;
  }

  private void initIfNeeded() {
    if (!initialized) {
      synchronized (this) {
        if (!initialized) {
          initAll();
        }
      }
    }
  }
}
