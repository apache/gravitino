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
package org.apache.gravitino.catalog.lakehouse.hudi.backend;

import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.hudi.ops.HudiCatalogBackendOps;

/** Base class for Hudi catalog backends. */
public abstract class HudiCatalogBackend {

  private final BackendType backendType;

  private final HudiCatalogBackendOps backendOps;

  public abstract void initialize(Map<String, String> properties);

  protected HudiCatalogBackend(BackendType backendType, HudiCatalogBackendOps backendOps) {
    this.backendType = backendType;
    this.backendOps = backendOps;
  }

  public BackendType type() {
    return backendType;
  }

  public HudiCatalogBackendOps backendOps() {
    return backendOps;
  }
}
