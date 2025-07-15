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
package org.apache.gravitino.iceberg.integration.test.authorization;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.iceberg.integration.test.IcebergRESTServiceBaseIT;
import org.apache.iceberg.rest.RESTClient;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class IcebergRESTAuthBaseIT extends IcebergRESTServiceBaseIT {

  public static final Logger LOG = LoggerFactory.getLogger(IcebergRESTAuthBaseIT.class);

  protected static final String NAMESPACE = "iceberg_auth_test";
  protected static final String ADMIN_USER = "admin";
  protected static final String NORMAL_USER = "normal_user";

  protected SparkSession adminSparkSession;
  protected SparkSession normalUserSparkSession;

  protected RESTClient adminClient;
  protected RESTClient normalUserClient;

  @Override
  protected void initEnv() {
    // enable auth
    // setup an Iceberg REST server in aux mode
  }

  Map<String, String> getCatalogConfig() {
    Map<String, String> configMap = new HashMap<>();
    // put configs
    return configMap;
  }
}
