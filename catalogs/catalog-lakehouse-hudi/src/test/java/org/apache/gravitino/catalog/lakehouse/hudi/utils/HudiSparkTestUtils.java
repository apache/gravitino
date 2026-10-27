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
package org.apache.gravitino.catalog.lakehouse.hudi.utils;

import java.lang.reflect.Field;

/** Test helpers for Spark sessions that create Hudi tables. */
public final class HudiSparkTestUtils {

  private HudiSparkTestUtils() {}

  /**
   * Drops Hudi's JVM-wide Hive metadata client.
   *
   * <p>Hudi's {@code HiveClientUtils.getSingletonClientForMetadata} caches the first Hive client it
   * builds and keeps it after the Spark session closes. Test classes in one JVM that talk to
   * different metastores must clear it, or Hudi's CREATE TABLE goes to the previous class's
   * metastore.
   */
  public static void resetHudiHiveClient() {
    try {
      Class<?> utils = Class.forName("org.apache.spark.sql.hive.HiveClientUtils$");
      Object module = utils.getField("MODULE$").get(null);
      Field client = utils.getDeclaredField("client");
      client.setAccessible(true);
      client.set(module, null);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Cannot reset Hudi's singleton Hive client", e);
    }
  }
}
