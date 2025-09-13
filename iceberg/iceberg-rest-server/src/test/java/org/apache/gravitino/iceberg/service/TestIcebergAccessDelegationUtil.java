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
package org.apache.gravitino.iceberg.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class TestIcebergAccessDelegationUtil {

  @Test
  void testIsCredentialVendingWithNull() {
    assertFalse(IcebergAccessDelegationUtil.isCredentialVending(null));
  }

  @Test
  void testIsCredentialVendingWithVendedCredentials() {
    assertTrue(IcebergAccessDelegationUtil.isCredentialVending("vended-credentials"));
    assertTrue(IcebergAccessDelegationUtil.isCredentialVending("VENDED-CREDENTIALS"));
    assertTrue(IcebergAccessDelegationUtil.isCredentialVending("Vended-Credentials"));
  }

  @Test
  void testIsCredentialVendingWithRemoteSigning() {
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> IcebergAccessDelegationUtil.isCredentialVending("remote-signing"));
    assertEquals(
        "Gravitino IcebergRESTServer doesn't support remote signing", exception.getMessage());
  }

  @Test
  void testIsCredentialVendingWithInvalidValue() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> IcebergAccessDelegationUtil.isCredentialVending("invalid-value"));
    assertTrue(
        exception.getMessage().contains("X-Iceberg-Access-Delegation: invalid-value is illegal"));
    assertTrue(
        exception
            .getMessage()
            .contains("Iceberg REST spec supports: [vended-credentials,remote-signing]"));
    assertTrue(
        exception
            .getMessage()
            .contains("Gravitino Iceberg REST server supports: vended-credentials"));
  }
}
