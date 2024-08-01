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
package org.apache.gravitino.spark.connector.version;

import com.google.common.collect.ImmutableMap;
import java.util.Locale;
import java.util.Map;
import org.apache.spark.package$;
import org.apache.spark.util.VersionUtils$;

public class CatalogNameAdaptor {
  private static final Map<String, String> catalogNames =
      ImmutableMap.of(
          "hive-3.3", "org.apache.gravitino.spark.connector.hive.GravitinoHiveCatalogSpark33",
          "hive-3.4", "org.apache.gravitino.spark.connector.hive.GravitinoHiveCatalogSpark34",
          "hive-3.5", "org.apache.gravitino.spark.connector.hive.GravitinoHiveCatalogSpark35",
          "lakehouse-iceberg-3.3",
              "org.apache.gravitino.spark.connector.iceberg.GravitinoIcebergCatalogSpark33",
          "lakehouse-iceberg-3.4",
              "org.apache.gravitino.spark.connector.iceberg.GravitinoIcebergCatalogSpark34",
          "lakehouse-iceberg-3.5",
              "org.apache.gravitino.spark.connector.iceberg.GravitinoIcebergCatalogSpark35");

  private static String sparkVersion() {
    return package$.MODULE$.SPARK_VERSION();
  }

  private static String getCatalogName(String provider, int majorVersion, int minorVersion) {
    String key =
        String.format("%s-%d.%d", provider.toLowerCase(Locale.ROOT), majorVersion, minorVersion);
    return catalogNames.get(key);
  }

  public static String getCatalogName(String provider) {
    int majorVersion = VersionUtils$.MODULE$.majorVersion(sparkVersion());
    int minorVersion = VersionUtils$.MODULE$.minorVersion(sparkVersion());
    return getCatalogName(provider, majorVersion, minorVersion);
  }
}
