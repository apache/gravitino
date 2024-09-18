/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.credential;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.storage.S3Properties;

public class CredentialUtils {
  private static Map<String, String> icebergCredentialPropertyMap =
      ImmutableMap.of(
          S3Properties.GRAVITINO_S3_ACCESS_KEY_ID, IcebergConstants.ICEBERG_S3_ACCESS_KEY_ID,
          S3Properties.GRAVITINO_S3_SECRET_ACCESS_KEY,
              IcebergConstants.ICEBERG_S3_SECRET_ACCESS_KEY,
          S3Properties.GRAVITINO_S3_TOKEN, IcebergConstants.ICEBERG_S3_TOKEN);

  public static Map<String, String> toIcebergProperties(Credential credential) {
    if (credential instanceof S3TokenCredential || credential instanceof S3SecretKeyCredential) {
      return transformProperties(credential.getCredentialInfo(), icebergCredentialPropertyMap);
    }
    throw new UnsupportedOperationException(
        "Couldn't convert " + credential.getCredentialType() + " credential to Iceberg properties");
  }

  private static Map<String, String> transformProperties(
      Map<String, String> originProperties, Map<String, String> transformMap) {
    HashMap<String, String> properties = new HashMap();
    originProperties.forEach(
        (k, v) -> {
          if (transformMap.containsKey(k)) {
            properties.put(transformMap.get(k), v);
          }
        });
    return properties;
  }
}
