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
package org.apache.gravitino.trino.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.trino.spi.TrinoException;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TestGravitinoConnectorFactory {

  @Test
  void testSecuritySensitivePropertyNames() {
    GravitinoConnectorFactory factory = new GravitinoConnectorFactory(null);
    Map<String, String> config =
        Map.ofEntries(
            Map.entry("trino.jdbc.password", "password"),
            Map.entry("gravitino.client.oauth2.clientSecret", "secret"),
            Map.entry("hive.s3.aws-access-key", "access-key"),
            Map.entry("hive.s3.aws-secret-key", "secret-key"),
            Map.entry("authentication.token", "token"),
            Map.entry("oauth2.credential", "credential"),
            Map.entry("tls.private_key", "private-key"),
            Map.entry("gravitino.uri", "uri"));

    Set<String> sensitivePropertyNames =
        factory.getSecuritySensitivePropertyNames("catalog", config, null);

    assertThat(sensitivePropertyNames)
        .containsExactlyInAnyOrder(
            "trino.jdbc.password",
            "gravitino.client.oauth2.clientSecret",
            "hive.s3.aws-access-key",
            "hive.s3.aws-secret-key",
            "authentication.token",
            "oauth2.credential",
            "tls.private_key");
    assertThatThrownBy(() -> sensitivePropertyNames.add("another.password"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void testParseTrinoSpiVersion() {
    assertThat(GravitinoConnectorFactory.parseTrinoSpiVersion("478")).isEqualTo(478);
    assertThat(GravitinoConnectorFactory.parseTrinoSpiVersion("478-e")).isEqualTo(478);
    assertThat(GravitinoConnectorFactory.parseTrinoSpiVersion("478.1-vendor")).isEqualTo(478);
  }

  @Test
  void testRejectInvalidTrinoSpiVersion() {
    assertThatThrownBy(() -> GravitinoConnectorFactory.parseTrinoSpiVersion("starburst-478"))
        .isInstanceOf(TrinoException.class)
        .hasMessage("Invalid Trino SPI version 'starburst-478': expected leading digits");
  }
}
