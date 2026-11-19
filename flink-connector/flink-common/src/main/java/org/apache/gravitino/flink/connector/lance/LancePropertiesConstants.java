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

/** Constants used for Lance catalog and table property configuration in Apache Flink. */
public final class LancePropertiesConstants {

  /** Lance REST server endpoint URI property key. */
  public static final String GRAVITINO_LANCE_CATALOG_URI = "uri";

  /** Lance catalog backend property key. */
  public static final String GRAVITINO_LANCE_CATALOG_BACKEND = "catalog-backend";

  /** Lance catalog implementation property key. */
  public static final String GRAVITINO_LANCE_CATALOG_IMPL = "impl";

  /** Lance catalog warehouse base location property key. */
  public static final String GRAVITINO_LANCE_CATALOG_WAREHOUSE = "warehouse";

  /** Lance catalog storage location property key. */
  public static final String GRAVITINO_LANCE_CATALOG_LOCATION = "location";

  /** REST catalog backend identifier. */
  public static final String LANCE_CATALOG_BACKEND_REST = "rest";

  /** Directory catalog backend identifier. */
  public static final String LANCE_CATALOG_BACKEND_DIR = "dir";

  /** Flink Lance connector identifier. */
  public static final String LANCE_CONNECTOR_TYPE = "lance";

  /** Flink table connector option key. */
  public static final String FLINK_CONNECTOR = "connector";

  /** Flink Lance table path option key. */
  public static final String LANCE_TABLE_PATH = "path";

  /** Gravitino table location property key. */
  public static final String LANCE_TABLE_LOCATION = "location";

  /** Prefix for Lance storage configuration options. */
  public static final String LANCE_STORAGE_PREFIX = "lance.storage.";

  /** Lance table format identifier. */
  public static final String LANCE_TABLE_FORMAT = "lance";

  private LancePropertiesConstants() {}
}
