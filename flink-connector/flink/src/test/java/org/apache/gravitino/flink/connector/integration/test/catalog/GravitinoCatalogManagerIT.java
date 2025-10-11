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
package org.apache.gravitino.flink.connector.integration.test.catalog;

import java.util.Collections;
import org.apache.gravitino.flink.connector.catalog.GravitinoCatalogManager;
import org.apache.gravitino.flink.connector.integration.test.FlinkEnvIT;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("gravitino-docker-test")
public class GravitinoCatalogManagerIT extends FlinkEnvIT {

  @Override
  protected String getProvider() {
    return null;
  }

  @Test
  public void testCreateGravitinoCatalogManager() {
    String gravitinoUri = "http://127.0.0.1:8090";
    GravitinoCatalogManager manager1 =
        Assertions.assertDoesNotThrow(
            () ->
                GravitinoCatalogManager.create(
                    gravitinoUri, GRAVITINO_METALAKE, Collections.emptyMap()));
    Assertions.assertDoesNotThrow(manager1::listCatalogs);
    GravitinoCatalogManager manager2 =
        Assertions.assertDoesNotThrow(
            () ->
                GravitinoCatalogManager.create(
                    gravitinoUri, GRAVITINO_METALAKE, Collections.emptyMap()));
    Assertions.assertDoesNotThrow(manager2::listCatalogs);
  }
}
