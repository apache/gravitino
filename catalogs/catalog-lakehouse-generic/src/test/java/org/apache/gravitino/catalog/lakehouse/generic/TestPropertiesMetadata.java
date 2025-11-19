/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.catalog.lakehouse.generic;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.Schema;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.rel.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestPropertiesMetadata {
  public static GenericCatalog genericCatalog;

  @BeforeAll
  static void init() {
    genericCatalog = new GenericCatalog();
  }

  @Test
  void testCatalogPropertiesMetadata() {
    PropertiesMetadata catalogPropertiesMetadata = genericCatalog.catalogPropertiesMetadata();
    Assertions.assertNotNull(catalogPropertiesMetadata);

    Map<String, String> catalogProperties = ImmutableMap.of("location", "/tmp/test1");

    String catalogLocation =
        (String)
            catalogPropertiesMetadata.getOrDefault(
                catalogProperties, GenericCatalog.PROPERTY_LOCATION);
    Assertions.assertEquals("/tmp/test1", catalogLocation);
  }

  @Test
  void testSchemaPropertiesMetadata() {
    PropertiesMetadata schemaPropertiesMetadata = genericCatalog.schemaPropertiesMetadata();
    Assertions.assertNotNull(schemaPropertiesMetadata);

    Map<String, String> schemaProperties = ImmutableMap.of("location", "/tmp/test_schema");

    String schemaLocation =
        (String) schemaPropertiesMetadata.getOrDefault(schemaProperties, Schema.PROPERTY_LOCATION);
    Assertions.assertEquals("/tmp/test_schema", schemaLocation);
  }

  @Test
  void testTablePropertiesMetadata() {
    PropertiesMetadata tablePropertiesMetadata = genericCatalog.tablePropertiesMetadata();
    Assertions.assertNotNull(tablePropertiesMetadata);

    Map<String, String> tableProperties =
        ImmutableMap.of(
            "lance.storage.type", "s3",
            "lance.storage.s3.bucket", "my-bucket",
            "lance.storage.s3.region", "us-west-2",
            "location", "/tmp/test_table",
            "format", "iceberg");

    String tableLocation =
        (String) tablePropertiesMetadata.getOrDefault(tableProperties, Table.PROPERTY_LOCATION);
    Assertions.assertEquals("/tmp/test_table", tableLocation);
  }
}
