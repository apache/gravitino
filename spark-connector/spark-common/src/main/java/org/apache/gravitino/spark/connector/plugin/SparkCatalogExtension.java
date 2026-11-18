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

/**
 * A catalog binding contributed by a jar outside this connector, discovered through {@link
 * java.util.ServiceLoader}.
 *
 * <p>A build that carries no in-tree class for a {@link
 * org.apache.gravitino.spark.connector.catalog.SparkCatalogKind}, such as Paimon on a Scala version
 * Paimon publishes no artifact for, can still support it by shipping a separate jar with a {@code
 * META-INF/services/org.apache.gravitino.spark.connector.plugin.SparkCatalogExtension} entry. See
 * {@link SparkBindings.Builder#build()} for how discovered extensions are merged with the bindings
 * a version module supplies at compile time.
 */
public interface SparkCatalogExtension {

  /**
   * Returns the Gravitino catalog provider this extension serves, such as {@code lakehouse-paimon}.
   * Resolved to a {@link org.apache.gravitino.spark.connector.catalog.SparkCatalogKind} through
   * {@link org.apache.gravitino.spark.connector.catalog.SparkCatalogKind#fromProvider(String)}.
   *
   * @return the Gravitino catalog provider
   */
  String provider();

  /**
   * Returns the Spark catalog class name to register for {@link #provider()}.
   *
   * @return the fully qualified Spark catalog class name
   * @throws IllegalStateException if this jar has no catalog class for the running Spark version
   */
  String catalogClassName();
}
