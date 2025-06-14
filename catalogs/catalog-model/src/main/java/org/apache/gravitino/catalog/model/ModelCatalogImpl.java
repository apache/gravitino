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
package org.apache.gravitino.catalog.model;

import java.util.Map;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.capability.Capability;

public class ModelCatalogImpl extends BaseCatalog<ModelCatalogImpl> {

  private static final ModelCatalogPropertiesMetadata CATALOG_PROPERTIES_META =
      new ModelCatalogPropertiesMetadata();

  private static final ModelSchemaPropertiesMetadata SCHEMA_PROPERTIES_META =
      new ModelSchemaPropertiesMetadata();

  private static final ModelPropertiesMetadata MODEL_PROPERTIES_META =
      new ModelPropertiesMetadata();

  private static final ModelVersionPropertiesMetadata MODEL_VERSION_PROPERTIES_META =
      new ModelVersionPropertiesMetadata();

  @Override
  public String shortName() {
    return "model";
  }

  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    EntityStore store = GravitinoEnv.getInstance().entityStore();
    return new ModelCatalogOperations(store);
  }

  @Override
  public PropertiesMetadata catalogPropertiesMetadata() throws UnsupportedOperationException {
    return CATALOG_PROPERTIES_META;
  }

  @Override
  public PropertiesMetadata schemaPropertiesMetadata() throws UnsupportedOperationException {
    return SCHEMA_PROPERTIES_META;
  }

  @Override
  public PropertiesMetadata modelPropertiesMetadata() throws UnsupportedOperationException {
    return MODEL_PROPERTIES_META;
  }

  @Override
  public PropertiesMetadata modelVersionPropertiesMetadata() throws UnsupportedOperationException {
    return MODEL_VERSION_PROPERTIES_META;
  }

  @Override
  protected Capability newCapability() {
    return new ModelCatalogCapability();
  }
}
