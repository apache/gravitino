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
package org.apache.gravitino.iceberg.experimental;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.iceberg.RESTService;
import org.apache.gravitino.iceberg.common.IcebergConfig;

/** Iceberg REST auxiliary service backed by the experimental Iceberg dependency set. */
public class ExperimentalRESTService extends RESTService {

  /** The experimental Iceberg REST auxiliary service name. */
  public static final String SERVICE_NAME = "iceberg-rest-experimental";

  /** {@inheritDoc} */
  @Override
  public String shortName() {
    return SERVICE_NAME;
  }

  /**
   * Initializes the experimental service with the regular Iceberg REST configuration as its base.
   * Experimental service properties take precedence when the same key is configured under both
   * service prefixes.
   *
   * @param properties experimental auxiliary service properties
   * @param auxMode whether the service is running as an auxiliary service
   */
  @Override
  public void serviceInit(Map<String, String> properties, boolean auxMode) {
    Map<String, String> regularProperties =
        GravitinoEnv.getInstance()
            .config()
            .getConfigsWithPrefix(IcebergConfig.ICEBERG_CONFIG_PREFIX);
    super.serviceInit(mergeProperties(regularProperties, properties), auxMode);
  }

  static Map<String, String> mergeProperties(
      Map<String, String> regularProperties, Map<String, String> experimentalProperties) {
    Map<String, String> mergedProperties = new HashMap<>(regularProperties);
    mergedProperties.putAll(experimentalProperties);
    return mergedProperties;
  }
}
