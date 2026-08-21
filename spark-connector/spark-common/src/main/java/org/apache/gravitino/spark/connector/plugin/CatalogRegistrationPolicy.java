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

package org.apache.gravitino.spark.connector.plugin;

import org.apache.gravitino.annotation.DeveloperApi;

/** Decides whether an advertised REST catalog is registered, and under what Spark name. */
@DeveloperApi
public interface CatalogRegistrationPolicy {

  /**
   * Returns whether to register an advertised catalog automatically.
   *
   * @param format the lakehouse format that advertised the catalog
   * @param catalogName the catalog name advertised by the format's REST server
   * @return true to register the catalog, false to skip it
   */
  boolean shouldRegister(String format, String catalogName);

  /**
   * Returns the Spark catalog name for an accepted catalog.
   *
   * @param format the lakehouse format that advertised the catalog
   * @param catalogName the accepted catalog name
   * @return the Spark catalog name
   */
  default String registeredCatalogName(String format, String catalogName) {
    return catalogName;
  }
}
