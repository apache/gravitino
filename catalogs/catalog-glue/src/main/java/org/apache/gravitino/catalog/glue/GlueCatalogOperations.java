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
package org.apache.gravitino.catalog.glue;

import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;

/**
 * Operations implementation for the AWS Glue Data Catalog connector.
 *
 * <p>This is a scaffold stub. Full implementation will be added in PR-02 through PR-06.
 *
 * <p>TODO PR-04: implement SupportsSchemas (Schema CRUD + exception mapping)
 *
 * <p>TODO PR-05: implement TableCatalog (Table CRUD + type detection)
 */
public class GlueCatalogOperations implements CatalogOperations {

  // TODO PR-04: add GlueClient field and catalogId

  @Override
  public void initialize(
      Map<String, String> config, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    // TODO PR-04: build GlueClient via GlueClientProvider and store catalogId
  }

  @Override
  public void testConnection(
      NameIdentifier catalogIdent,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws Exception {
    // TODO PR-04: call GlueClient.getDatabases() to verify connectivity
  }

  @Override
  public void close() throws IOException {
    // TODO PR-04: close GlueClient
  }
}
