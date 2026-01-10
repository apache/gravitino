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
package org.apache.gravitino.flink.connector.hive;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.util.Constants;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.RCFileStorageFormatDescriptor;
import org.apache.hadoop.hive.ql.io.StorageFormatDescriptor;
import org.apache.hadoop.hive.ql.io.StorageFormatFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestHivePropertiesConverter {

  private static final HivePropertiesConverter CONVERTER =
      new HivePropertiesConverter(new HiveConf());

  @Test
  public void testToGravitinoCatalogProperties() {
    Configuration configuration =
        Configuration.fromMap(
            ImmutableMap.of(
                "hive-conf-dir",
                "src/test/resources/flink-tests",
                "flink.bypass.key",
                "value",
                HiveConf.ConfVars.METASTOREURIS.varname,
                "thrift://127.0.0.1:9084"));
    Map<String, String> properties = CONVERTER.toGravitinoCatalogProperties(configuration);
    Assertions.assertEquals(3, properties.size());
    Assertions.assertEquals(
        "src/test/resources/flink-tests",
        properties.get("flink.bypass.hive-conf-dir"),
        "This will add the prefix");
    Assertions.assertEquals(
        "value", properties.get("flink.bypass.key"), "The prefix have already existed");
    Assertions.assertEquals(
        "thrift://127.0.0.1:9084",
        properties.get(HiveConstants.METASTORE_URIS),
        "The key is converted to Gravitino Config");
  }

  @Test
  public void testToFlinkCatalogProperties() {
    Map<String, String> catalogProperties =
        ImmutableMap.of("flink.bypass.key", "value", "metastore.uris", "thrift://xxx");
    Map<String, String> flinkCatalogProperties =
        CONVERTER.toFlinkCatalogProperties(catalogProperties);
    Assertions.assertEquals(3, flinkCatalogProperties.size());
    Assertions.assertEquals("value", flinkCatalogProperties.get("key"));
    Assertions.assertEquals(
        GravitinoHiveCatalogFactoryOptions.IDENTIFIER, flinkCatalogProperties.get("type"));
    Assertions.assertEquals("thrift://xxx", flinkCatalogProperties.get("hive.metastore.uris"));
  }

  @Test
  public void testToGravitinoTablePropertiesDefaultStorageFormat() {
    HiveConf hiveConf = new HiveConf();
    HivePropertiesConverter converter = new HivePropertiesConverter(hiveConf);
    Map<String, String> properties = converter.toGravitinoTableProperties(Collections.emptyMap());

    String defaultFormat = hiveConf.getVar(HiveConf.ConfVars.HIVEDEFAULTFILEFORMAT);
    StorageFormatDescriptor descriptor = new StorageFormatFactory().get(defaultFormat);
    String expectedSerde = descriptor.getSerde();
    if (expectedSerde == null && descriptor instanceof RCFileStorageFormatDescriptor) {
      expectedSerde = hiveConf.getVar(HiveConf.ConfVars.HIVEDEFAULTRCFILESERDE);
    }
    if (expectedSerde == null) {
      expectedSerde = hiveConf.getVar(HiveConf.ConfVars.HIVEDEFAULTSERDE);
    }

    Assertions.assertEquals(defaultFormat, properties.get(HiveConstants.FORMAT));
    Assertions.assertEquals(
        descriptor.getInputFormat(), properties.get(HiveConstants.INPUT_FORMAT));
    Assertions.assertEquals(
        descriptor.getOutputFormat(), properties.get(HiveConstants.OUTPUT_FORMAT));
    Assertions.assertEquals(expectedSerde, properties.get(HiveConstants.SERDE_LIB));
  }

  @Test
  public void testToFlinkTablePropertiesDefaultStorageFormat() {
    HiveConf hiveConf = new HiveConf();
    HivePropertiesConverter converter = new HivePropertiesConverter(hiveConf);
    Map<String, String> properties =
        converter.toFlinkTableProperties(
            Collections.emptyMap(), Collections.emptyMap(), new ObjectPath("default", "test"));

    String defaultFormat = hiveConf.getVar(HiveConf.ConfVars.HIVEDEFAULTFILEFORMAT);
    StorageFormatDescriptor descriptor = new StorageFormatFactory().get(defaultFormat);
    String expectedSerde = descriptor.getSerde();
    if (expectedSerde == null && descriptor instanceof RCFileStorageFormatDescriptor) {
      expectedSerde = hiveConf.getVar(HiveConf.ConfVars.HIVEDEFAULTRCFILESERDE);
    }
    if (expectedSerde == null) {
      expectedSerde = hiveConf.getVar(HiveConf.ConfVars.HIVEDEFAULTSERDE);
    }

    Assertions.assertEquals(Constants.IDENTIFIER, properties.get("connector"));
    Assertions.assertEquals(defaultFormat, properties.get(Constants.STORED_AS_FILE_FORMAT));
    Assertions.assertEquals(
        descriptor.getInputFormat(), properties.get(Constants.STORED_AS_INPUT_FORMAT));
    Assertions.assertEquals(
        descriptor.getOutputFormat(), properties.get(Constants.STORED_AS_OUTPUT_FORMAT));
    Assertions.assertEquals(expectedSerde, properties.get(Constants.SERDE_LIB_CLASS_NAME));
  }
}
