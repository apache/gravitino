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

package org.apache.gravitino.credential;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class TestRoleSessionNameUtils {
  @Test
  void sessionNamesAreValidAndDistinguishable() {
    String nonAscii = new String(Character.toChars(0x1F600));
    String first = RoleSessionNameUtils.create("gravitino_irsa_session_", "first name/" + nonAscii);
    String second =
        RoleSessionNameUtils.create("gravitino_irsa_session_", "first name:" + nonAscii);
    assertTrue(first.matches("[A-Za-z0-9_@.-]{2,64}"));
    assertNotEquals(first, second);
    assertEquals(
        first, RoleSessionNameUtils.create("gravitino_irsa_session_", "first name/" + nonAscii));

    String longName = "firstname.lastname.contractor@subsidiary.example.com";
    assertTrue(RoleSessionNameUtils.create("gravitino_irsa_session_", longName).length() <= 64);
    assertTrue(RoleSessionNameUtils.create("gravitino_", longName).length() <= 64);
  }

  @Test
  void replacesCharactersRejectedByAliyun() {
    String sessionName =
        RoleSessionNameUtils.create("gravitino_", "alice+etl=prod,team@example.com");

    assertTrue(sessionName.matches("[A-Za-z0-9_@.-]{2,64}"));
    assertTrue(sessionName.startsWith("gravitino_alice_etl_prod_team@example.com_"));
  }
}
