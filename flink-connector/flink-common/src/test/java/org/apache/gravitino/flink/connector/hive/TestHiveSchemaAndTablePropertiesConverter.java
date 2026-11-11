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
import org.apache.flink.table.catalog.hive.util.Constants;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.catalog.hive.HiveStorageConstants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestHiveSchemaAndTablePropertiesConverter {

  @Test
  public void testDefaultFormatSerdeApplied() {
    HiveSchemaAndTablePropertiesConverter converter =
        new HiveSchemaAndTablePropertiesConverter(defaultHiveConf("ORC"));
    Map<String, String> properties = converter.toGravitinoTableProperties(Collections.emptyMap());
    Assertions.assertEquals("ORC", properties.get(HiveConstants.FORMAT));
    Assertions.assertEquals(
        HiveStorageConstants.ORC_SERDE_CLASS, properties.get(HiveConstants.SERDE_LIB));
  }

  @Test
  public void testRowFormatOverridesDefaultFormatSerde() {
    HiveSchemaAndTablePropertiesConverter converter =
        new HiveSchemaAndTablePropertiesConverter(defaultHiveConf("ORC"));
    Map<String, String> properties =
        converter.toGravitinoTableProperties(
            ImmutableMap.of(Constants.SERDE_LIB_CLASS_NAME, "com.acme.CustomSerde"));
    Assertions.assertEquals("ORC", properties.get(HiveConstants.FORMAT));
    Assertions.assertEquals("com.acme.CustomSerde", properties.get(HiveConstants.SERDE_LIB));
  }

  @Test
  public void testStoredAsOverridesRowFormatSerde() {
    HiveSchemaAndTablePropertiesConverter converter =
        new HiveSchemaAndTablePropertiesConverter(defaultHiveConf("ORC"));
    Map<String, String> properties =
        converter.toGravitinoTableProperties(
            ImmutableMap.of(
                Constants.SERDE_LIB_CLASS_NAME,
                "com.acme.CustomSerde",
                Constants.STORED_AS_FILE_FORMAT,
                "ORC"));
    Assertions.assertEquals("ORC", properties.get(HiveConstants.FORMAT));
    Assertions.assertEquals(
        HiveStorageConstants.ORC_SERDE_CLASS, properties.get(HiveConstants.SERDE_LIB));
  }

  @Test
  public void testStoredAsInputOutputCopiedWhenNoFileFormat() {
    HiveSchemaAndTablePropertiesConverter converter =
        new HiveSchemaAndTablePropertiesConverter(defaultHiveConf("ORC"));
    Map<String, String> properties =
        converter.toGravitinoTableProperties(
            ImmutableMap.of(
                Constants.STORED_AS_INPUT_FORMAT,
                "inputFormat",
                Constants.STORED_AS_OUTPUT_FORMAT,
                "outputFormat"));
    Assertions.assertEquals("inputFormat", properties.get(HiveConstants.INPUT_FORMAT));
    Assertions.assertEquals("outputFormat", properties.get(HiveConstants.OUTPUT_FORMAT));
  }

  @Test
  public void testInputOutputFromStoredAsFormat() {
    HiveSchemaAndTablePropertiesConverter converter =
        new HiveSchemaAndTablePropertiesConverter(defaultHiveConf("ORC"));
    Map<String, String> properties =
        converter.toGravitinoTableProperties(
            ImmutableMap.of(Constants.STORED_AS_FILE_FORMAT, "ORC"));
    Assertions.assertEquals(
        HiveStorageConstants.ORC_INPUT_FORMAT_CLASS, properties.get(HiveConstants.INPUT_FORMAT));
    Assertions.assertEquals(
        HiveStorageConstants.ORC_OUTPUT_FORMAT_CLASS, properties.get(HiveConstants.OUTPUT_FORMAT));
  }

  @Test
  public void testInputOutputFromTablePropertiesBeforeDefault() {
    HiveSchemaAndTablePropertiesConverter converter =
        new HiveSchemaAndTablePropertiesConverter(defaultHiveConf("ORC"));
    Map<String, String> properties =
        converter.toGravitinoTableProperties(
            ImmutableMap.of(
                HiveConstants.INPUT_FORMAT,
                "customInput",
                HiveConstants.OUTPUT_FORMAT,
                "customOutput"));
    Assertions.assertEquals("customInput", properties.get(HiveConstants.INPUT_FORMAT));
    Assertions.assertEquals("customOutput", properties.get(HiveConstants.OUTPUT_FORMAT));
  }

  @Test
  public void testValidateStorageFormat() {
    HiveSchemaAndTablePropertiesConverter converter =
        new HiveSchemaAndTablePropertiesConverter(defaultHiveConf("ORC"));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            converter.toGravitinoTableProperties(
                ImmutableMap.of(Constants.STORED_AS_FILE_FORMAT, "NotAFormat")));
  }

  private static HiveConf defaultHiveConf(String defaultFormat) {
    HiveConf hiveConf = new HiveConf();
    hiveConf.setVar(HiveConf.ConfVars.HIVEDEFAULTFILEFORMAT, defaultFormat);
    hiveConf.setVar(
        HiveConf.ConfVars.HIVEDEFAULTSERDE, HiveStorageConstants.LAZY_SIMPLE_SERDE_CLASS);
    return hiveConf;
  }
}
