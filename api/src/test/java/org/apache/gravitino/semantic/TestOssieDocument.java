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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class TestOssieDocument {

  @Test
  public void testFactoriesPreserveContentAndDeclareFormat() {
    String yaml = "version: 0.2.0.dev0\nname: sales\n";
    OssieDocument yamlDocument = OssieDocument.yaml(yaml);
    assertEquals(yaml, yamlDocument.content());
    assertEquals(OssieFormat.YAML, yamlDocument.format());

    String json = "{\"version\":\"0.2.0.dev0\",\"name\":\"sales\"}";
    OssieDocument jsonDocument = OssieDocument.json(json);
    assertEquals(json, jsonDocument.content());
    assertEquals(OssieFormat.JSON, jsonDocument.format());
  }

  @Test
  public void testConstructionDoesNotParseContent() {
    assertEquals("", OssieDocument.yaml("").content());
    assertEquals("not json", OssieDocument.json("not json").content());
  }

  @Test
  public void testNullContentIsRejected() {
    assertThrows(IllegalArgumentException.class, () -> OssieDocument.yaml(null));
    assertThrows(IllegalArgumentException.class, () -> OssieDocument.json(null));
  }

  @Test
  public void testEqualityIncludesContentAndFormat() {
    OssieDocument document = OssieDocument.json("{}");
    assertEquals(document, document);
    assertEquals(document, OssieDocument.json("{}"));
    assertEquals(document.hashCode(), OssieDocument.json("{}").hashCode());
    assertNotEquals(document, OssieDocument.yaml("{}"));
    assertNotEquals(document, OssieDocument.json("[]"));
    assertNotEquals(document, null);
    assertNotEquals(document, "{}");
  }
}
