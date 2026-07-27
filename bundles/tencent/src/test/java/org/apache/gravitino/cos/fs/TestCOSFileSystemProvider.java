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

package org.apache.gravitino.cos.fs;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.credential.COSSecretKeyCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.storage.COSProperties;
import org.apache.hadoop.fs.CosNConfigKeys;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCOSFileSystemProvider {

  @Test
  void testSchemeAndName() {
    COSFileSystemProvider provider = new COSFileSystemProvider();
    Assertions.assertEquals("cosn", provider.scheme());
    Assertions.assertEquals("cos", provider.name());
  }

  @Test
  void testGravitinoToHadoopKeyMapping() {
    Map<String, String> mapping = COSFileSystemProvider.GRAVITINO_KEY_TO_COS_HADOOP_KEY;

    Assertions.assertEquals(
        CosNConfigKeys.COSN_REGION_KEY, mapping.get(COSProperties.GRAVITINO_COS_REGION));
    Assertions.assertEquals(
        CosNConfigKeys.COSN_ENDPOINT_SUFFIX_KEY, mapping.get(COSProperties.GRAVITINO_COS_ENDPOINT));
    Assertions.assertEquals(
        CosNConfigKeys.COSN_USERINFO_SECRET_ID_KEY,
        mapping.get(COSProperties.GRAVITINO_COS_ACCESS_KEY_ID));
    Assertions.assertEquals(
        CosNConfigKeys.COSN_USERINFO_SECRET_KEY_KEY,
        mapping.get(COSProperties.GRAVITINO_COS_ACCESS_KEY_SECRET));
  }

  @Test
  void testGetFileSystemCredentialConfWithSecretKey() {
    COSFileSystemProvider provider = new COSFileSystemProvider();
    Credential[] credentials = new Credential[] {new COSSecretKeyCredential("ak", "sk")};

    Map<String, String> conf = provider.getFileSystemCredentialConf(credentials);

    Assertions.assertEquals(
        COSCredentialsProvider.class.getCanonicalName(),
        conf.get(CosNConfigKeys.COSN_CREDENTIALS_PROVIDER));
  }

  @Test
  void testGetFileSystemCredentialConfWithEmptyCredentials() {
    COSFileSystemProvider provider = new COSFileSystemProvider();

    Map<String, String> conf = provider.getFileSystemCredentialConf(new Credential[0]);

    Assertions.assertTrue(
        conf.isEmpty(),
        "No credentials should yield an empty conf map (no credentials provider injected)");
  }

  @Test
  void testKeyMappingDoesNotLeakUnknownProperties() {
    // Sanity: random non-COS properties must not appear in the Gravitino->Hadoop mapping.
    Map<String, String> mapping = COSFileSystemProvider.GRAVITINO_KEY_TO_COS_HADOOP_KEY;
    Map<String, String> notMapped = new HashMap<>();
    notMapped.put("oss-region", "value");
    notMapped.put("s3-region", "value");
    for (String key : notMapped.keySet()) {
      Assertions.assertFalse(
          mapping.containsKey(key),
          "Gravitino->Hadoop COS mapping should not contain unrelated key: " + key);
    }
  }
}
