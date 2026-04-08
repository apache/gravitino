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

import java.util.Map;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.PropertiesMetadata;

/** Implementation of an AWS Glue catalog in Apache Gravitino. */
public class GlueCatalog extends BaseCatalog<GlueCatalog> {

  static final GlueCatalogPropertiesMetadata CATALOG_PROPERTIES_METADATA =
      new GlueCatalogPropertiesMetadata();

  static final GlueSchemaPropertiesMetadata SCHEMA_PROPERTIES_METADATA =
      new GlueSchemaPropertiesMetadata();

  static final GlueTablePropertiesMetadata TABLE_PROPERTIES_METADATA =
      new GlueTablePropertiesMetadata();

  @Override
  public String shortName() {
    return "glue";
  }

  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    return new GlueCatalogOperations();
  }

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    return CATALOG_PROPERTIES_METADATA;
  }

  @Override
  public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
    return SCHEMA_PROPERTIES_METADATA;
  }

  @Override
  public PropertiesMetadata tablePropertiesMetadata() throws UnsupportedOperationException {
    return TABLE_PROPERTIES_METADATA;
  }
}