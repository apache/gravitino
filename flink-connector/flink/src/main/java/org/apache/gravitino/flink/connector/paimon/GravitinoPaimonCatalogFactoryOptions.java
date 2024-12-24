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

package org.apache.gravitino.flink.connector.paimon;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class GravitinoPaimonCatalogFactoryOptions {

  /** Identifier for the {@link GravitinoPaimonCatalog}. */
  public static final String IDENTIFIER = "gravitino-paimon";

  public static ConfigOption<String> CATALOG_BACKEND =
      ConfigOptions.key("catalog.backend")
          .stringType()
          .defaultValue("fileSystem")
          .withDescription("");

  public static ConfigOption<String> WAREHOUSE =
      ConfigOptions.key("warehouse").stringType().noDefaultValue();

  public static ConfigOption<String> URI = ConfigOptions.key("uri").stringType().noDefaultValue();

  public static ConfigOption<String> JDBC_USER =
      ConfigOptions.key("jdbc.user").stringType().noDefaultValue();

  public static ConfigOption<String> JDBC_PASSWORD =
      ConfigOptions.key("jdbc.password").stringType().noDefaultValue();
}
