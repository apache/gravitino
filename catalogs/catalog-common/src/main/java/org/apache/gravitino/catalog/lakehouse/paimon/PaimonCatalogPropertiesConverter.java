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
import java.util.HashMap;
import java.util.Map;

/** Property conversion shared by the Gravitino Paimon catalog and its REST service. */
public final class PaimonCatalogPropertiesConverter {
  private static final Map<String, String> MAPPINGS =
      ImmutableMap.<String, String>builder()
          .put("catalog-backend", "metastore")
          .put("warehouse", "warehouse")
          .put("uri", "uri")
          .put("jdbc-user", "jdbc.user")
          .put("jdbc-password", "jdbc.password")
          .put("jdbc-driver", "jdbc-driver")
          .put("token-provider", "token.provider")
          .put("token", "token")
          .put("s3-access-key-id", "s3.access-key")
          .put("s3-secret-access-key", "s3.secret-key")
          .put("s3-endpoint", "s3.endpoint")
          .put("oss-access-key-id", "fs.oss.accessKeyId")
          .put("oss-secret-access-key", "fs.oss.accessKeySecret")
          .put("oss-endpoint", "fs.oss.endpoint")
          .put("dlf-access-key-id", "dlf.access-key-id")
          .put("dlf-access-key-secret", "dlf.access-key-secret")
          .put("dlf-security-token", "dlf.security-token")
          .put("dlf-token-path", "dlf.token-path")
          .put("dlf-token-loader", "dlf.token-loader")
          .put("authentication.type", "authentication.type")
          .put("authentication.kerberos.keytab-uri", "authentication.kerberos.keytab-uri")
          .put("authentication.kerberos.principal", "authentication.kerberos.principal")
          .put(
              "authentication.kerberos.check-interval-sec",
              "authentication.kerberos.check-interval-sec")
          .put(
              "authentication.kerberos.keytab-fetch-timeout-sec",
              "authentication.kerberos.keytab-fetch-timeout-sec")
          .build();

  private PaimonCatalogPropertiesConverter() {}

  /**
   * Converts the catalog's recognized configuration keys to native Paimon keys.
   *
   * @param properties Gravitino catalog properties
   * @param keepUnknown whether to retain unrecognized keys
   * @return converted properties
   */
  public static Map<String, String> toInnerProperties(
      Map<String, String> properties, boolean keepUnknown) {
    Map<String, String> result = new HashMap<>();
    properties.forEach(
        (key, value) -> {
          if (MAPPINGS.containsKey(key)) {
            result.put(MAPPINGS.get(key), value);
          } else if (keepUnknown) {
            result.put(key, value);
          }
        });
    return result;
  }
}
