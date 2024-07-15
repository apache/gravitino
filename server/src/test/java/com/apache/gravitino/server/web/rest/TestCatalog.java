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
package com.apache.gravitino.server.web.rest;

import com.apache.gravitino.connector.BaseCatalog;
import com.apache.gravitino.connector.CatalogInfo;
import com.apache.gravitino.connector.CatalogOperations;
import com.apache.gravitino.connector.HasPropertyMetadata;
import com.apache.gravitino.connector.PropertiesMetadata;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;

public class TestCatalog extends BaseCatalog<TestCatalog> {
  private static final PropertiesMetadata PROPERTIES_METADATA = ImmutableMap::of;

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
      public void close() throws IOException {}
    };
  }

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    return PROPERTIES_METADATA;
  }
}
