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

package org.apache.gravitino.flink.connector.catalog;

import org.apache.flink.table.factories.CatalogFactory;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.flink.connector.PartitionConverter;
import org.apache.gravitino.flink.connector.PropertiesConverter;

public interface BaseCatalogFactory extends CatalogFactory {

  /**
   * Define gravitino catalog provider {@link org.apache.gravitino.CatalogProvider}.
   *
   * @return The requested gravitino catalog provider.
   */
  String gravitinoCatalogProvider();

  /**
   * Define gravitino catalog type {@link Catalog.Type}.
   *
   * @return The requested gravitino catalog type.
   */
  Catalog.Type gravitinoCatalogType();

  /**
   * Define properties converter {@link PropertiesConverter}.
   *
   * @return The requested property converter.
   */
  PropertiesConverter propertiesConverter();

  /**
   * Define partition converter.
   *
   * @return The requested partition converter.
   */
  PartitionConverter partitionConverter();
}
