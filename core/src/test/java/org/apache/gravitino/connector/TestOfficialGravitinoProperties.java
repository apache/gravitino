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
package org.apache.gravitino.connector;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestOfficialGravitinoProperties {

  @Test
  void testCloudAndConnectorKeysAreDefined() {
    Assertions.assertTrue(OfficialGravitinoProperties.isDefined("s3-access-key-id"));
    Assertions.assertTrue(OfficialGravitinoProperties.isDefined("aws-access-key-id"));
    Assertions.assertTrue(OfficialGravitinoProperties.isDefined("dlf-access-key-id"));
    Assertions.assertTrue(OfficialGravitinoProperties.isDefined("credential-providers"));
    Assertions.assertTrue(OfficialGravitinoProperties.isDefined("location-unknown"));
    Assertions.assertTrue(OfficialGravitinoProperties.isDefined("location-warehouse"));
    Assertions.assertFalse(OfficialGravitinoProperties.isDefined("typo-access-key"));
    Assertions.assertFalse(OfficialGravitinoProperties.isDefined(null));
  }

  @Test
  void testHiddenMatchesOfficialSecretsNotIdentifiers() {
    Assertions.assertFalse(OfficialGravitinoProperties.isHidden("s3-access-key-id"));
    Assertions.assertFalse(OfficialGravitinoProperties.isHidden("aws-access-key-id"));
    Assertions.assertTrue(OfficialGravitinoProperties.isHidden("s3-secret-access-key"));
    Assertions.assertTrue(OfficialGravitinoProperties.isHidden("aws-secret-access-key"));
    Assertions.assertTrue(OfficialGravitinoProperties.isHidden("jdbc-password"));
  }
}
