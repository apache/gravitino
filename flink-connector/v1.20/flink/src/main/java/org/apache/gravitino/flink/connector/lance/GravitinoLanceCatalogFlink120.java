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

package org.apache.gravitino.flink.connector.lance;

import java.util.Map;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;
import org.apache.gravitino.flink.connector.utils.CatalogCompat;
import org.apache.gravitino.flink.connector.utils.CatalogCompatFlink120;

/** {@link GravitinoLanceCatalog} implementation for Flink 1.20. */
public class GravitinoLanceCatalogFlink120 extends GravitinoLanceCatalog {

  /**
   * Constructs a new {@link GravitinoLanceCatalogFlink120}.
   *
   * @param catalogName the catalog name
   * @param defaultDatabase the default database name
   * @param schemaAndTablePropertiesConverter the properties converter
   * @param partitionConverter the partition converter
   * @param catalogOptions the catalog options
   */
  public GravitinoLanceCatalogFlink120(
      String catalogName,
      String defaultDatabase,
      SchemaAndTablePropertiesConverter schemaAndTablePropertiesConverter,
      PartitionConverter partitionConverter,
      Map<String, String> catalogOptions) {
    super(
        catalogName,
        defaultDatabase,
        schemaAndTablePropertiesConverter,
        partitionConverter,
        catalogOptions);
  }

  @Override
  protected CatalogCompat catalogCompat() {
    return CatalogCompatFlink120.INSTANCE;
  }
}
