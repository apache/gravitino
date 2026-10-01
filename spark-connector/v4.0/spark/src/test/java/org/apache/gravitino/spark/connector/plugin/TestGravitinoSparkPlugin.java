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

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.spark.connector.GravitinoSparkConfig;
import org.apache.gravitino.spark.connector.jdbc.GravitinoJdbcCatalogSpark40;
import org.apache.spark.SparkConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests that this module's bindings are complete. Constructing the driver plugin runs {@code
 * SparkBindings.build()}, whose Preconditions reject a missing binding, so a catalog this module
 * forgot to bind fails here rather than at session startup.
 */
public class TestGravitinoSparkPlugin {

  @Test
  void testTheBindingsThisModuleDeclaresAreComplete() {
    Assertions.assertNotNull(new GravitinoSparkPlugin().driverPlugin());
  }

  @Test
  void testDorisFallsBackByDefaultAndRejectsSpecializedMode() {
    Catalog catalog = Mockito.mock(Catalog.class);
    Mockito.when(catalog.provider()).thenReturn("jdbc-doris");
    GravitinoDriverPlugin plugin =
        (GravitinoDriverPlugin) new GravitinoSparkPlugin().driverPlugin();
    SparkConf genericConf = new SparkConf(false);
    plugin.registerOptInExtensions(genericConf);

    plugin.registerGravitinoCatalogs(genericConf, ImmutableMap.of("doris", catalog));

    Assertions.assertEquals(
        GravitinoJdbcCatalogSpark40.class.getName(), genericConf.get("spark.sql.catalog.doris"));

    SparkConf specializedConf = new SparkConf(false);
    specializedConf.set(GravitinoSparkConfig.GRAVITINO_ENABLE_DORIS_SUPPORT, "true");
    plugin.registerOptInExtensions(specializedConf);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> plugin.registerGravitinoCatalogs(specializedConf, ImmutableMap.of("doris", catalog)));
  }
}
