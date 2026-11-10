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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.gravitino.NameIdentifier;
import org.junit.jupiter.api.Test;

public class TestSemanticModelChange {

  @Test
  public void testCreateChanges() {
    SemanticModelDefinition definition = definition("orders");

    SemanticModelChange.RenameSemanticModel rename =
        (SemanticModelChange.RenameSemanticModel) SemanticModelChange.rename("new_name");
    SemanticModelChange.UpdateComment updateComment =
        (SemanticModelChange.UpdateComment) SemanticModelChange.updateComment(null);
    SemanticModelChange.SetProperty setProperty =
        (SemanticModelChange.SetProperty) SemanticModelChange.setProperty("owner", "analytics");
    SemanticModelChange.RemoveProperty removeProperty =
        (SemanticModelChange.RemoveProperty) SemanticModelChange.removeProperty("owner");
    SemanticModelChange.ReplaceDefinition replaceDefinition =
        (SemanticModelChange.ReplaceDefinition) SemanticModelChange.replaceDefinition(definition);

    assertEquals("new_name", rename.getNewName());
    assertNull(updateComment.getNewComment());
    assertEquals("owner", setProperty.getProperty());
    assertEquals("analytics", setProperty.getValue());
    assertEquals("owner", removeProperty.getProperty());
    assertEquals(definition, replaceDefinition.getDefinition());
  }

  @Test
  public void testChangesUseValueSemantics() {
    assertEquals(SemanticModelChange.rename("new_name"), SemanticModelChange.rename("new_name"));
    assertEquals(
        SemanticModelChange.updateComment("comment"), SemanticModelChange.updateComment("comment"));
    assertEquals(
        SemanticModelChange.setProperty("key", "value"),
        SemanticModelChange.setProperty("key", "value"));
    assertEquals(
        SemanticModelChange.removeProperty("key"), SemanticModelChange.removeProperty("key"));
    assertEquals(
        SemanticModelChange.replaceDefinition(definition("orders")),
        SemanticModelChange.replaceDefinition(definition("orders")));
  }

  @Test
  public void testRejectInvalidChanges() {
    assertThrows(IllegalArgumentException.class, () -> SemanticModelChange.rename(""));
    assertThrows(
        IllegalArgumentException.class, () -> SemanticModelChange.setProperty("", "value"));
    assertThrows(
        IllegalArgumentException.class, () -> SemanticModelChange.setProperty("key", null));
    assertThrows(IllegalArgumentException.class, () -> SemanticModelChange.removeProperty(""));
    assertThrows(IllegalArgumentException.class, () -> SemanticModelChange.replaceDefinition(null));
  }

  private static SemanticModelDefinition definition(String datasetName) {
    return SemanticModelDefinition.builder()
        .withDatasets(
            new Dataset[] {
              Dataset.builder()
                  .withName(datasetName)
                  .withSource(NameIdentifier.of("sales", "mart", datasetName))
                  .build()
            })
        .build();
  }
}
