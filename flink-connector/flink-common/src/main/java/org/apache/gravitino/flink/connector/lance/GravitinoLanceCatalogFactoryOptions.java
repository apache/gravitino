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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.catalog.CommonCatalogOptions;

/** Configuration options for {@link GravitinoLanceCatalogFactory}. */
public final class GravitinoLanceCatalogFactoryOptions {

  /** Factory identifier for the Gravitino Lance catalog. */
  public static final String IDENTIFIER = "gravitino-lance";

  /** Default database configuration option. */
  public static final ConfigOption<String> DEFAULT_DATABASE =
      CommonCatalogOptions.DEFAULT_DATABASE_KEY;

  /** Lance REST server endpoint URI configuration option. */
  public static final ConfigOption<String> URI =
      ConfigOptions.key(LancePropertiesConstants.GRAVITINO_LANCE_CATALOG_URI)
          .stringType()
          .noDefaultValue()
          .withDescription("The Lance REST server endpoint URI.");

  /** Lance catalog backend configuration option. */
  public static final ConfigOption<String> CATALOG_BACKEND =
      ConfigOptions.key(LancePropertiesConstants.GRAVITINO_LANCE_CATALOG_BACKEND)
          .stringType()
          .defaultValue(LancePropertiesConstants.LANCE_CATALOG_BACKEND_REST)
          .withDescription("The Lance catalog backend type (default: rest).");

  /** Lance catalog implementation configuration option. */
  public static final ConfigOption<String> IMPL =
      ConfigOptions.key(LancePropertiesConstants.GRAVITINO_LANCE_CATALOG_IMPL)
          .stringType()
          .defaultValue(LancePropertiesConstants.LANCE_CATALOG_BACKEND_REST)
          .withDescription("The Lance catalog implementation (default: rest).");

  /** Lance catalog warehouse location configuration option. */
  public static final ConfigOption<String> WAREHOUSE =
      ConfigOptions.key(LancePropertiesConstants.GRAVITINO_LANCE_CATALOG_WAREHOUSE)
          .stringType()
          .noDefaultValue()
          .withDescription("The warehouse or base location for Lance tables.");

  /** Lance catalog storage location configuration option. */
  public static final ConfigOption<String> LOCATION =
      ConfigOptions.key(LancePropertiesConstants.GRAVITINO_LANCE_CATALOG_LOCATION)
          .stringType()
          .noDefaultValue()
          .withDescription("The storage location for Lance catalog.");

  private GravitinoLanceCatalogFactoryOptions() {}
}
