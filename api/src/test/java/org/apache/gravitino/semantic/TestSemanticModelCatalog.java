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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchSemanticModelException;
import org.junit.jupiter.api.Test;

public class TestSemanticModelCatalog {

  @Test
  public void testSemanticModelExists() {
    SemanticModelCatalog catalog = new ExistsOnlySemanticModelCatalog();

    assertTrue(catalog.semanticModelExists(NameIdentifier.of("schema", "existing")));
    assertFalse(catalog.semanticModelExists(NameIdentifier.of("schema", "missing")));
  }

  private static class ExistsOnlySemanticModelCatalog implements SemanticModelCatalog {

    @Override
    public NameIdentifier[] listSemanticModels(Namespace namespace) {
      throw new UnsupportedOperationException();
    }

    @Override
    public SemanticModel loadSemanticModel(NameIdentifier ident) {
      if ("missing".equals(ident.name())) {
        throw new NoSuchSemanticModelException("Semantic Model does not exist: %s", ident);
      }
      return null;
    }

    @Override
    public SemanticModel createSemanticModel(
        NameIdentifier ident,
        @Nullable String comment,
        SemanticModelDefinition definition,
        Map<String, String> properties) {
      throw new UnsupportedOperationException();
    }

    @Override
    public SemanticModel alterSemanticModel(NameIdentifier ident, SemanticModelChange... changes) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean dropSemanticModel(NameIdentifier ident) {
      throw new UnsupportedOperationException();
    }
  }
}
