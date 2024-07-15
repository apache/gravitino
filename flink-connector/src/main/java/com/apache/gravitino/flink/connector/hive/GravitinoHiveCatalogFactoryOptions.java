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
package com.apache.gravitino.flink.connector.hive;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.hadoop.hive.conf.HiveConf;

public class GravitinoHiveCatalogFactoryOptions {

  /** Identifier for the {@link GravitinoHiveCatalog}. */
  public static final String IDENTIFIER = "gravitino-hive";

  public static final ConfigOption<String> HIVE_METASTORE_URIS =
      ConfigOptions.key(HiveConf.ConfVars.METASTOREURIS.varname)
          .stringType()
          .noDefaultValue()
          .withDescription(
              "The Hive metastore URIs, it is higher priority than hive.metastore.uris in hive-site.xml");
}
