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
package org.apache.gravitino.server.web.rest;

import static org.apache.gravitino.connector.BaseCatalogPropertiesMetadata.BASIC_CATALOG_PROPERTIES_METADATA;

import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.PropertiesMetadata;

public class TestCatalog extends BaseCatalog<TestCatalog> {

  @Override
  public String shortName() {
    return "test";
  }

  @Override
  protected CatalogOperations newOps(Map config) {
    return new CatalogOperations() {

      @Override
      public void initialize(
          Map<String, String> config, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
          throws RuntimeException {}

      @Override
      public void testConnection(
          NameIdentifier catalogIdent,
          Type type,
          String provider,
          String comment,
          Map<String, String> properties)
          throws Exception {}

      @Override
      public void close() throws IOException {}
    };
  }

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    return BASIC_CATALOG_PROPERTIES_METADATA;
  }
}
