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

package org.apache.gravitino.spark.connector.plugin.restcatalog;

import java.util.List;
import java.util.Map;

/** Provides format-specific discovery and Spark configuration for lakehouse REST catalogs. */
public interface LakehouseRESTCatalogProvider {

  /**
   * Returns the format token used in {@code spark.sql.gravitino.<format>REST.*} configuration.
   *
   * @return the format token
   */
  String format();

  /**
   * Lists the catalog names advertised by the format's REST server.
   *
   * @param uri the configured REST server URI
   * @param catalogProperties global catalog properties configured for the format
   * @return advertised catalog names
   */
  List<String> listCatalogs(String uri, Map<String, String> catalogProperties);

  /**
   * Returns the Spark catalog implementation class name.
   *
   * @return the Spark catalog implementation class name
   */
  String catalogClassName();

  /**
   * Returns provider-generated Spark catalog property suffixes and values.
   *
   * @param uri the configured REST server URI
   * @param advertisedCatalogName the catalog name advertised by the REST server
   * @return generated property suffixes and values
   */
  Map<String, String> generatedCatalogProperties(String uri, String advertisedCatalogName);

  /**
   * Returns Spark session extension class names required by the provider.
   *
   * @return Spark session extension class names
   */
  String[] sparkExtensions();
}
