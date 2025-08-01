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

package org.apache.gravitino.spark.connector;

import static org.apache.gravitino.spark.connector.plugin.GravitinoDriverPlugin.extractGravitinoClientConfig;

import java.util.Map;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestGravitinoSparkConfig {

  @Test
  void testClientConfig() {
    Map<String, String> noClientConfig = extractGravitinoClientConfig(new SparkConf());
    Assertions.assertTrue(noClientConfig.isEmpty());

    SparkConf sparkConf =
        new SparkConf()
            .set(GravitinoSparkConfig.GRAVITINO_CLIENT_CONFIG_PREFIX + "socketTimeoutMs", "1000")
            .set(
                GravitinoSparkConfig.GRAVITINO_CLIENT_CONFIG_PREFIX + "connectionTimeoutMs",
                "2000");
    Map<String, String> clientConfig = extractGravitinoClientConfig(sparkConf);
    Assertions.assertEquals(clientConfig.get("gravitino.client.socketTimeoutMs"), "1000");
    Assertions.assertEquals(clientConfig.get("gravitino.client.connectionTimeoutMs"), "2000");
  }
}
