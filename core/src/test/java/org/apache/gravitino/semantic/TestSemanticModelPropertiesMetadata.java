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

import static org.apache.gravitino.semantic.SemanticModel.DEFAULT_OSSIE_VERSION;
import static org.apache.gravitino.semantic.SemanticModel.PROPERTY_OSSIE_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.gravitino.connector.PropertyEntry;
import org.junit.jupiter.api.Test;

/** Tests the property metadata shared by Semantic Models. */
public class TestSemanticModelPropertiesMetadata {

  private static final SemanticModelPropertiesMetadata METADATA =
      new SemanticModelPropertiesMetadata();

  @Test
  public void testOssieVersionProperty() {
    PropertyEntry<?> entry = METADATA.getPropertyEntry(PROPERTY_OSSIE_VERSION);

    assertFalse(entry.isRequired());
    assertFalse(entry.isImmutable());
    assertEquals(DEFAULT_OSSIE_VERSION, entry.getDefaultValue());
    assertEquals("future-version", entry.decode("future-version"));
    assertThrows(IllegalArgumentException.class, () -> entry.decode(" "));
  }
}
