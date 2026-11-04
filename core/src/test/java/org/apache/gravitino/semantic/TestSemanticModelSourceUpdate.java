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
package org.apache.gravitino.semantic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.junit.jupiter.api.Test;

class TestSemanticModelSourceUpdate {
  @Test
  void testImmutableCompleteReplacement() {
    SemanticModelSource source = new SemanticModelSource("Orders", Entity.EntityType.TABLE, 123);
    List<SemanticModelSource> mutable = new ArrayList<>(List.of(source));
    SemanticModelSourceUpdate update = SemanticModelSourceUpdate.replace(mutable);
    mutable.clear();
    assertEquals(List.of(source), update.sources());
    assertTrue(update.replacesSources());
    assertThrows(UnsupportedOperationException.class, () -> update.sources().clear());
    update.validate(definition("Orders"));
    assertThrows(IllegalArgumentException.class, () -> update.validate(definition("orders")));
    Dataset dataset = definition("Orders").datasets()[0];
    assertThrows(
        IllegalArgumentException.class,
        () ->
            update.validate(
                SemanticModelDefinition.builder()
                    .withDatasets(new Dataset[] {dataset, dataset})
                    .build()));
    assertThrows(
        IllegalArgumentException.class,
        () -> SemanticModelSourceUpdate.replace(List.of(source, source)));
    assertFalse(SemanticModelSourceUpdate.preserve().replacesSources());
    assertThrows(
        IllegalArgumentException.class,
        () -> new SemanticModelSource("Orders", Entity.EntityType.CATALOG, 123));
    assertThrows(
        IllegalArgumentException.class,
        () -> new SemanticModelSource("", Entity.EntityType.TABLE, 123));
  }

  private SemanticModelDefinition definition(String name) {
    return SemanticModelDefinition.builder()
        .withDatasets(
            new Dataset[] {
              Dataset.builder()
                  .withName(name)
                  .withSource(NameIdentifier.of("catalog", "schema", "orders"))
                  .build()
            })
        .build();
  }
}
