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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Verifies the catalog and REST service share backend, authentication and storage conversion. */
public class TestPaimonCatalogPropertiesConverter {
  @Test
  void testKnownPropertiesAndUnknownRetention() {
    Map<String, String> properties =
        ImmutableMap.of(
            "catalog-backend",
            "rest",
            "token-provider",
            "bear",
            "dlf-token-loader",
            "loader",
            "authentication.type",
            "kerberos",
            "unknown",
            "retained");
    Map<String, String> converted =
        PaimonCatalogPropertiesConverter.toInnerProperties(properties, false);
    Assertions.assertEquals("rest", converted.get("metastore"));
    Assertions.assertEquals("bear", converted.get("token.provider"));
    Assertions.assertEquals("loader", converted.get("dlf.token-loader"));
    Assertions.assertEquals("kerberos", converted.get("authentication.type"));
    Assertions.assertFalse(converted.containsKey("unknown"));
    Assertions.assertEquals(
        "retained",
        PaimonCatalogPropertiesConverter.toInnerProperties(properties, true).get("unknown"));
    Assertions.assertEquals("rest", properties.get("catalog-backend"));
  }
}
