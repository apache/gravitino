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
package org.apache.gravitino.catalog.fileset;

import java.util.Map;
import org.apache.gravitino.connector.BaseCatalog;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.capability.Capability;

/**
 * Hadoop catalog is a fileset catalog that can manage filesets on the Hadoop Compatible File
 * Systems, like Local, HDFS, S3, ADLS, etc., using the Hadoop FileSystem API. It can manage
 * filesets from different Hadoop Compatible File Systems in the same catalog.
 */
public class FilesetCatalogImpl extends BaseCatalog<FilesetCatalogImpl> {

  static final FilesetCatalogPropertiesMetadata CATALOG_PROPERTIES_META =
      new FilesetCatalogPropertiesMetadata();

  static final FilesetSchemaPropertiesMetadata SCHEMA_PROPERTIES_META =
      new FilesetSchemaPropertiesMetadata();

  static final FilesetPropertiesMetadata FILESET_PROPERTIES_META = new FilesetPropertiesMetadata();

  @Override
  public String shortName() {
    return "fileset";
  }

  @Override
  protected CatalogOperations newOps(Map<String, String> config) {
    return new SecureFilesetCatalogOperations();
  }

  @Override
  protected Capability newCapability() {
    return new FilesetCatalogCapability();
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
  public PropertiesMetadata filesetPropertiesMetadata() throws UnsupportedOperationException {
    return FILESET_PROPERTIES_META;
  }
}
