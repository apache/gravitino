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

package org.apache.gravitino.cli.commands;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.gravitino.secret.SupportsSecrets;
import org.junit.jupiter.api.Test;

public class TestListProperties {

  @Test
  public void testPropertiesWithSecretsOverlaysMaskedValues() {
    SupportsSecrets secrets = mock(SupportsSecrets.class);
    when(secrets.getSecrets()).thenReturn(Map.of("jdbc-password", "secret"));

    Map<String, String> merged =
        ListProperties.propertiesWithSecrets(
            Map.of("jdbc-password", "******", "jdbc-url", "jdbc:x"), secrets);

    assertEquals("secret", merged.get("jdbc-password"));
    assertEquals("jdbc:x", merged.get("jdbc-url"));
  }

  @Test
  public void testPropertiesWithSecretsNullSafe() {
    assertEquals(Map.of(), ListProperties.propertiesWithSecrets(null, null));
    assertEquals(Map.of("a", "1"), ListProperties.propertiesWithSecrets(Map.of("a", "1"), null));
  }
}
