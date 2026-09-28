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
package org.apache.gravitino.iceberg.experimental;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;
import org.apache.gravitino.auxiliary.GravitinoAuxiliaryService;
import org.junit.jupiter.api.Test;

/** Tests for {@link ExperimentalRESTService}. */
public class TestExperimentalRESTService {

  @Test
  public void testShortName() {
    assertEquals(ExperimentalRESTService.SERVICE_NAME, new ExperimentalRESTService().shortName());
  }

  @Test
  public void testDiscoverService() {
    ServiceLoader<GravitinoAuxiliaryService> services =
        ServiceLoader.load(GravitinoAuxiliaryService.class);

    assertTrue(
        StreamSupport.stream(services.spliterator(), false)
            .anyMatch(
                service ->
                    ExperimentalRESTService.SERVICE_NAME.equalsIgnoreCase(service.shortName())));
  }

  @Test
  public void testExperimentalPropertiesOverrideRegularProperties() {
    Map<String, String> mergedProperties =
        ExperimentalRESTService.mergeProperties(
            Map.of("warehouse", "regular", "catalog-backend", "memory"),
            Map.of("warehouse", "experimental", "classpath", "experimental/libs"));

    assertEquals("experimental", mergedProperties.get("warehouse"));
    assertEquals("memory", mergedProperties.get("catalog-backend"));
    assertEquals("experimental/libs", mergedProperties.get("classpath"));
  }
}
