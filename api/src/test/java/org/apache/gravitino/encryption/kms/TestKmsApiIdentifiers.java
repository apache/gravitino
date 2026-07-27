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
package org.apache.gravitino.encryption.kms;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestKmsApiIdentifiers {

  @Test
  void testAcceptsLowercaseKebabCase() {
    Assertions.assertEquals("aws-kms", KmsApiIdentifiers.requireValid("aws-kms"));
    Assertions.assertEquals("test", KmsApiIdentifiers.requireValid("test"));
    Assertions.assertEquals("acme-kms-v2", KmsApiIdentifiers.requireValid("acme-kms-v2"));
  }

  @Test
  void testRejectsBlankAndPaddedValues() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> KmsApiIdentifiers.requireValid(null));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> KmsApiIdentifiers.requireValid(""));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> KmsApiIdentifiers.requireValid(" "));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> KmsApiIdentifiers.requireValid(" aws-kms"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> KmsApiIdentifiers.requireValid("aws-kms "));
  }

  @Test
  void testRejectsNonKebabCaseValues() {
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> KmsApiIdentifiers.requireValid("AWS-KMS"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> KmsApiIdentifiers.requireValid("aws_kms"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> KmsApiIdentifiers.requireValid("aws kms"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> KmsApiIdentifiers.requireValid("aws--kms"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> KmsApiIdentifiers.requireValid("-aws-kms"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> KmsApiIdentifiers.requireValid("aws-kms-"));
  }
}
