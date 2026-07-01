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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestIcebergAccessDelegation {

  @Test
  void testParseEmptyHeader() {
    IcebergAccessDelegation delegation = IcebergAccessDelegation.parse(null);
    Assertions.assertFalse(delegation.requestVendedCredentials());
    Assertions.assertFalse(delegation.requestRemoteSigning());
  }

  @Test
  void testParseVendedCredentials() {
    IcebergAccessDelegation delegation = IcebergAccessDelegation.parse("vended-credentials");
    Assertions.assertTrue(delegation.requestVendedCredentials());
    Assertions.assertFalse(delegation.requestRemoteSigning());
  }

  @Test
  void testParseRemoteSigning() {
    IcebergAccessDelegation delegation = IcebergAccessDelegation.parse("remote-signing");
    Assertions.assertFalse(delegation.requestVendedCredentials());
    Assertions.assertTrue(delegation.requestRemoteSigning());
  }

  @Test
  void testParseBothModes() {
    IcebergAccessDelegation delegation =
        IcebergAccessDelegation.parse("vended-credentials, remote-signing");
    Assertions.assertTrue(delegation.requestVendedCredentials());
    Assertions.assertTrue(delegation.requestRemoteSigning());
  }

  @Test
  void testParseInvalidValue() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> IcebergAccessDelegation.parse("unsupported-mode"));
    Assertions.assertTrue(exception.getMessage().contains("illegal"));
  }
}
