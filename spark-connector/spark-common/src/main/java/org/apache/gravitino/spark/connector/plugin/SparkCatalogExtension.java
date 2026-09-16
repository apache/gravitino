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
package org.apache.gravitino.spark.connector.plugin;

import java.util.ServiceLoader;

/**
 * Service provider interface that lets a jar outside the connector supply the Spark catalog
 * implementation for a Gravitino catalog provider. Implementations are discovered through {@link
 * ServiceLoader} and take precedence over the catalogs the connector ships itself. The {@code
 * lakehouse-iceberg} and {@code lakehouse-paimon} providers stay behind their opt-in flags: an
 * extension for either is only consulted once the corresponding flag is enabled. When several
 * extensions declare the same provider, the first one discovered is used and the others are ignored
 * with a warning.
 */
public interface SparkCatalogExtension {

  /**
   * The Gravitino catalog provider this extension serves, for example {@code jdbc-oracle}.
   *
   * @return the provider name, matched case-insensitively
   */
  String provider();

  /**
   * The fully qualified name of the Spark {@code TableCatalog} class registered for the provider.
   * Spark loads it when the session is created, so it must be on the Spark class path.
   *
   * @return the Spark catalog class name
   */
  String catalogClassName();
}
