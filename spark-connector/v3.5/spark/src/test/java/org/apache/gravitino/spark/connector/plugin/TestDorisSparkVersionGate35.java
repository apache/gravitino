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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.spark.connector.version.CatalogNameAdaptor;
import org.apache.spark.SparkConf;
import org.apache.spark.package$;
import org.junit.jupiter.api.Test;

/** Verifies the Doris registration boundary against the Spark version on the test classpath. */
public class TestDorisSparkVersionGate35 {

  @Test
  void testRuntimeSparkPatchVersionGate() {
    assertFalse(GravitinoDriverPlugin.isDorisSparkVersionSupported("3.5.2"));
    assertTrue(GravitinoDriverPlugin.isDorisSparkVersionSupported("3.5.3"));
    assertTrue(GravitinoDriverPlugin.isDorisSparkVersionSupported("3.5.9"));
    assertTrue(GravitinoDriverPlugin.isDorisSparkVersionSupported("3.5.10"));
    assertFalse(GravitinoDriverPlugin.isDorisSparkVersionSupported("3.6.0"));

    Catalog catalog = mock(Catalog.class);
    when(catalog.provider()).thenReturn("jdbc-doris");
    GravitinoDriverPlugin plugin = new GravitinoDriverPlugin();
    plugin.setDorisSupportEnabled(true);

    if (CatalogNameAdaptor.getCatalogName("jdbc-doris") != null
        && GravitinoDriverPlugin.isDorisSparkVersionSupported(package$.MODULE$.SPARK_VERSION())) {
      assertDoesNotThrow(
          () ->
              plugin.registerGravitinoCatalogs(
                  new SparkConf(false), ImmutableMap.of("doris", catalog)));
    } else {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              plugin.registerGravitinoCatalogs(
                  new SparkConf(false), ImmutableMap.of("doris", catalog)));
    }
  }
}
