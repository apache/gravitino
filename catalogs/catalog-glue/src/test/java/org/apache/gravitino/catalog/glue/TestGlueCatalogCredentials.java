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
package org.apache.gravitino.catalog.glue;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

public class TestGlueCatalogCredentials {

  @Test
  void testValidateOnCreateRunsLocalConfigChecks() {
    GlueCatalog catalog =
        new GlueCatalog()
            .withCatalogConf(
                Map.of(
                    GlueConstants.AWS_REGION,
                    "us-east-1",
                    GlueConstants.AWS_ACCESS_KEY_ID,
                    "AKIAEXAMPLE"));

    assertTrue(catalog.shouldValidateOnCreate());
    IllegalArgumentException partial =
        assertThrows(IllegalArgumentException.class, catalog::validateOnCreate);
    assertTrue(partial.getMessage().contains(GlueConstants.AWS_SECRET_ACCESS_KEY));

    GlueCatalog temporaryKey =
        new GlueCatalog()
            .withCatalogConf(
                Map.of(
                    GlueConstants.AWS_REGION,
                    "us-east-1",
                    GlueConstants.AWS_ACCESS_KEY_ID,
                    "ASIAEXAMPLE",
                    GlueConstants.AWS_SECRET_ACCESS_KEY,
                    "secret"));
    IllegalArgumentException temporary =
        assertThrows(IllegalArgumentException.class, temporaryKey::validateOnCreate);
    assertTrue(temporary.getMessage().contains("session token"), temporary.getMessage());

    new GlueCatalog()
        .withCatalogConf(Map.of(GlueConstants.AWS_REGION, "us-east-1"))
        .validateOnCreate();
  }
}
