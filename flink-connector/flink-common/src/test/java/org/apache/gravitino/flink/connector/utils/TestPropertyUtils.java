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

package org.apache.gravitino.flink.connector.utils;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link PropertyUtils}. */
public class TestPropertyUtils {

  @Test
  public void testExtractHadoopConfigurationRemovesHadoopOptions() {
    Map<String, String> options = new HashMap<>();
    options.put("warehouse", "file:/tmp/warehouse");
    options.put("hadoop.fs.oss.endpoint", "oss-endpoint");
    options.put("fs.s3a.access.key", "s3-key");
    options.put("dfs.client.use.datanode.hostname", "true");

    Configuration configuration = PropertyUtils.extractHadoopConfiguration(options);

    Assertions.assertEquals("file:/tmp/warehouse", options.get("warehouse"));
    Assertions.assertFalse(options.containsKey("hadoop.fs.oss.endpoint"));
    Assertions.assertFalse(options.containsKey("fs.s3a.access.key"));
    Assertions.assertFalse(options.containsKey("dfs.client.use.datanode.hostname"));
    Assertions.assertEquals("oss-endpoint", configuration.get("fs.oss.endpoint"));
    Assertions.assertEquals("s3-key", configuration.get("fs.s3a.access.key"));
    Assertions.assertEquals("true", configuration.get("dfs.client.use.datanode.hostname"));
  }
}
