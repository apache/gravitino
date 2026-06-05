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
package org.apache.gravitino.utils;

import java.net.URI;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRemoteUriValidator {
  private static final String ALLOW_LOCAL_ADDRESS_CONFIG = "test.allow-local-address";

  @Test
  public void testRejectLocalAddressesByDefault() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                RemoteUriValidator.validate(
                    new URI("http://127.0.0.1/"), false, ALLOW_LOCAL_ADDRESS_CONFIG));
    Assertions.assertTrue(exception.getMessage().contains("Gravitino server side"));
    Assertions.assertTrue(exception.getMessage().contains(ALLOW_LOCAL_ADDRESS_CONFIG));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RemoteUriValidator.validate(
                new URI("http://localhost/"), false, ALLOW_LOCAL_ADDRESS_CONFIG));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RemoteUriValidator.validate(
                new URI("http://169.254.169.254/"), false, ALLOW_LOCAL_ADDRESS_CONFIG));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RemoteUriValidator.validate(
                new URI("http://10.0.0.1/"), false, ALLOW_LOCAL_ADDRESS_CONFIG));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RemoteUriValidator.validate(
                new URI("http://172.16.0.1/"), false, ALLOW_LOCAL_ADDRESS_CONFIG));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RemoteUriValidator.validate(
                new URI("http://192.168.0.1/"), false, ALLOW_LOCAL_ADDRESS_CONFIG));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RemoteUriValidator.validate(
                new URI("http://100.100.100.200/"), false, ALLOW_LOCAL_ADDRESS_CONFIG));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            RemoteUriValidator.validate(
                new URI("http://[fd00::1]/"), false, ALLOW_LOCAL_ADDRESS_CONFIG));
  }

  @Test
  public void testAllowLocalAddressesWhenConfigured() {
    Assertions.assertDoesNotThrow(
        () ->
            RemoteUriValidator.validate(
                new URI("http://127.0.0.1/"), true, ALLOW_LOCAL_ADDRESS_CONFIG));
    Assertions.assertDoesNotThrow(
        () ->
            RemoteUriValidator.validate(
                new URI("http://localhost/"), true, ALLOW_LOCAL_ADDRESS_CONFIG));
    Assertions.assertDoesNotThrow(
        () ->
            RemoteUriValidator.validate(
                new URI("http://192.168.0.1/"), true, ALLOW_LOCAL_ADDRESS_CONFIG));
  }
}
