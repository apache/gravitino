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
package org.apache.gravitino.connector;

import java.io.Closeable;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.SupportsSchemas;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.rel.TableCatalog;

/**
 * A catalog operation interface that is used to trigger the operations of a catalog. This interface
 * should be mixed with other Catalog interface like {@link SupportsSchemas} to provide schema
 * operation, {@link TableCatalog} to support table operations, etc.
 */
@Evolving
public interface CatalogOperations extends Closeable {

  /**
   * Initialize the CatalogOperation with specified configuration. This method is called after
   * CatalogOperation object is created, but before any other method is called. The method is used
   * to initialize the connection to the underlying metadata source. RuntimeException will be thrown
   * if the initialization failed.
   *
   * @param config The configuration of this Catalog.
   * @param info The information of this Catalog.
   * @param propertiesMetadata The properties metadata of this Catalog.
   * @throws RuntimeException if the initialization failed.
   */
  void initialize(
      Map<String, String> config, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException;

  /**
   * Test whether a catalog can be created with the specified parameters, without actually creating
   * it.
   *
   * @param catalogIdent the name of the catalog.
   * @param type the type of the catalog.
   * @param provider the provider of the catalog.
   * @param comment the comment of the catalog.
   * @param properties the properties of the catalog.
   * @throws Exception if connection fails.
   */
  void testConnection(
      NameIdentifier catalogIdent,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws Exception;
}
