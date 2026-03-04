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

import org.apache.gravitino.spark.connector.hive.GravitinoHiveCatalogSpark33;
import org.apache.gravitino.spark.connector.iceberg.GravitinoIcebergCatalogSpark33;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCatalogNameAdaptor {
  @Test
  void testSpark33() {
    String hiveCatalogName = CatalogNameAdaptor.getCatalogName("hive");
    Assertions.assertEquals(GravitinoHiveCatalogSpark33.class.getName(), hiveCatalogName);

    String icebergCatalogName = CatalogNameAdaptor.getCatalogName("lakehouse-iceberg");
    Assertions.assertEquals(GravitinoIcebergCatalogSpark33.class.getName(), icebergCatalogName);

    String paimonCatalogName = CatalogNameAdaptor.getCatalogName("lakehouse-paimon");
    Assertions.assertEquals(
        "org.apache.gravitino.spark.connector.paimon.GravitinoPaimonCatalogSpark33",
        paimonCatalogName);
  }
}
