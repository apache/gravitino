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
package org.apache.gravitino.catalog.lakehouse.paimon;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.config.ConfigBuilder;
import org.apache.gravitino.config.ConfigEntry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link org.apache.gravitino.catalog.lakehouse.paimon.PaimonConfig}. */
public class TestPaimonConfig {

  @Test
  public void testLoadPaimonConfig() {
    ConfigEntry<String> testConf = new ConfigBuilder("k1").stringConf().create();
    Map<String, String> properties = ImmutableMap.of(testConf.getKey(), "v1");

    PaimonConfig paimonConfig = new PaimonConfig();
    paimonConfig.loadFromMap(properties, k -> k.startsWith("gravitino."));
    Assertions.assertNull(paimonConfig.get(testConf));

    paimonConfig = new PaimonConfig(properties);
    Assertions.assertEquals("v1", paimonConfig.get(testConf));
  }

  @Test
  public void testHadoopS3AOptionsAreAlsoPassedAsFsS3AOptions() {
    // Since Paimon 2.0, paimon-s3 serves s3a:// paths and reads fs.s3a. options, not hadoop.fs.s3a.
    Map<String, String> conf =
        ImmutableMap.of(
            "gravitino.bypass.hadoop.fs.s3a.access.key",
            "ak",
            "gravitino.bypass.hadoop.fs.s3a.endpoint",
            "http://localhost:4566",
            "gravitino.bypass.fs.s3a.endpoint",
            "http://explicit:4566");

    Map<String, String> paimonConf =
        PaimonCatalogOperations.toPaimonConfig(conf, PaimonCatalog.CATALOG_PROPERTIES_META)
            .getAllConfig();

    // The hadoop.fs.s3a. options stay for Hadoop S3A.
    Assertions.assertEquals("ak", paimonConf.get("hadoop.fs.s3a.access.key"));
    Assertions.assertEquals("http://localhost:4566", paimonConf.get("hadoop.fs.s3a.endpoint"));
    // They are copied to fs.s3a. for paimon-s3, without overriding an fs.s3a. option that is set.
    Assertions.assertEquals("ak", paimonConf.get("fs.s3a.access.key"));
    Assertions.assertEquals("http://explicit:4566", paimonConf.get("fs.s3a.endpoint"));
  }
}
