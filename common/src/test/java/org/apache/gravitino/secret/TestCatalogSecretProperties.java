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
package org.apache.gravitino.secret;

import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Catalog;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCatalogSecretProperties {

  @Test
  public void testGetSecretPropertiesUnsupported() {
    Catalog catalog =
        new Catalog() {
          @Override
          public String name() {
            return "c";
          }

          @Override
          public Type type() {
            return Type.RELATIONAL;
          }

          @Override
          public String provider() {
            return "jdbc-mysql";
          }

          @Override
          public String comment() {
            return null;
          }

          @Override
          public Map<String, String> properties() {
            return Map.of();
          }

          @Override
          public Audit auditInfo() {
            return null;
          }
        };
    Assertions.assertTrue(CatalogSecretProperties.getSecretProperties(catalog).isEmpty());
  }

  @Test
  public void testApplySecretProperties() {
    Catalog catalog =
        new Catalog() {
          @Override
          public String name() {
            return "c";
          }

          @Override
          public Type type() {
            return Type.RELATIONAL;
          }

          @Override
          public String provider() {
            return "jdbc-mysql";
          }

          @Override
          public String comment() {
            return null;
          }

          @Override
          public Map<String, String> properties() {
            return Map.of();
          }

          @Override
          public Audit auditInfo() {
            return null;
          }

          @Override
          public SupportsSecretProperties supportsSecretProperties() {
            return () -> Map.of("custom.secret", "plaintext");
          }
        };

    Map<String, String> target = new HashMap<>();
    target.put("existing", "value");
    CatalogSecretProperties.applySecretProperties(catalog, target);

    Assertions.assertEquals("value", target.get("existing"));
    Assertions.assertEquals("plaintext", target.get("custom.secret"));
  }
}
