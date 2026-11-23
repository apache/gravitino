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

package org.apache.gravitino.meta;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.glossary.TermLink;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/** Tests storage definition bounds and snapshot isolation. */
public class TestGlossaryTermEntity {
  private GlossaryTermEntity.Builder builder() {
    return GlossaryTermEntity.builder()
        .withId(1L)
        .withName("term")
        .withNamespace(Namespace.of("lake", Entity.SYSTEM_CATALOG_RESERVED_NAME, "glossary_term"))
        .withAuditInfo(
            AuditInfo.builder().withCreator("creator").withCreateTime(Instant.EPOCH).build());
  }

  @Test
  public void testDomains() {
    Assertions.assertArrayEquals(new String[0], builder().build().domains());
    GlossaryTermEntity term = builder().withDomains("Sales", "Finance").build();
    Assertions.assertArrayEquals(new String[] {"Finance", "Sales"}, term.domains());
    term.domains()[0] = "Changed";
    Assertions.assertArrayEquals(new String[] {"Finance", "Sales"}, term.domains());
    Assertions.assertArrayEquals(
        new String[] {"Finance", "Sales"},
        (String[]) term.fields().get(GlossaryTermEntity.DOMAINS));
    assertNullArgument("domains", () -> builder().withDomains((String[]) null).build());
  }

  @Test
  public void testNullArguments() {
    assertNullArgument("id", () -> builder().withId(null).build());
    assertNullArgument("namespace", () -> builder().withNamespace(null).build());
    assertNullArgument("auditInfo", () -> builder().withAuditInfo(null).build());
    assertNullArgument("status", () -> builder().withStatus(null).build());
    assertNullArgument("name", () -> builder().withName(null).build());
    assertNullArgument("synonyms", () -> builder().withSynonyms(null));
    assertNullArgument("links", () -> builder().withLinks(null));
    assertNullArgument("links[0]", () -> builder().withLinks(new TermLink[] {null}));
    assertNullArgument("properties", () -> builder().withProperties(null));
    assertNullArgument("name", () -> builder().withSynonyms(new String[] {null}).build());
    assertNullArgument("name", () -> GlossaryTermNames.display(null));
    assertNullArgument("name", () -> GlossaryTermNames.key(null));
    assertNullArgument("name", () -> GlossaryTermNames.validate(null));
    Assertions.assertDoesNotThrow(() -> builder().withDefinition(null).withSourceRef(null).build());
  }

  @Test
  public void testBounds() {
    Assertions.assertDoesNotThrow(
        () -> builder().withName("😀".repeat(128)).withDefinition("a".repeat(16384)).build());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> builder().withName("😀".repeat(129)).build());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> builder().withDefinition("界".repeat(5462)).build());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> builder().withSourceRef("x".repeat(129)).build());
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> builder().withSynonyms(new String[101]).build());
    Assertions.assertThrows(IllegalArgumentException.class, () -> builder().withName(" ").build());
    Assertions.assertThrows(IllegalArgumentException.class, () -> builder().withVersion(0).build());
    Assertions.assertEquals("active customer", GlossaryTermNames.key("　Active\t Customer　"));
  }

  @Test
  public void testLinearDisplayNormalization() {
    String internal = "a" + " ".repeat(1_000_000) + "b";
    Assertions.assertTimeoutPreemptively(
        Duration.ofSeconds(5),
        () -> {
          Assertions.assertEquals(internal, GlossaryTermNames.display(internal));
          Assertions.assertEquals("a b", GlossaryTermNames.key(internal));
          Assertions.assertThrows(
              IllegalArgumentException.class, () -> builder().withName(internal).build());
        });
  }

  @Test
  public void testUnicodeWhitespaceBoundaries() {
    Pattern whitespace = Pattern.compile("\\s", Pattern.UNICODE_CHARACTER_CLASS);
    for (int codePoint = 0; codePoint <= Character.MAX_CODE_POINT; codePoint++) {
      String boundary = new String(Character.toChars(codePoint));
      String value = boundary + "😀a" + boundary;
      boolean matches = whitespace.matcher(boundary).matches();
      Assertions.assertEquals(
          matches ? "😀a" : value, GlossaryTermNames.display(value), "Code point " + codePoint);
      if (matches) Assertions.assertEquals("", GlossaryTermNames.display(boundary.repeat(3)));
    }
    Assertions.assertEquals("", GlossaryTermNames.display(""));
    Assertions.assertThrows(IllegalArgumentException.class, () -> GlossaryTermNames.display(null));
  }

  @Test
  public void testSnapshotIsolation() {
    String[] synonyms = {"client"};
    GlossaryTermEntity.Builder b =
        builder()
            .withSynonyms(synonyms)
            .withLinks(new TermLink[] {TermLink.of(null, "https://example.org")})
            .withProperties(Map.of("k", "v"));
    GlossaryTermEntity first = b.build();
    synonyms[0] = "changed";
    first.synonyms()[0] = "changed";
    b.withName("other").build();
    Assertions.assertEquals("term", first.name());
    Assertions.assertEquals("client", first.synonyms()[0]);
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> first.properties().put("a", "b"));
    Assertions.assertEquals(Entity.EntityType.GLOSSARY_TERM, first.type());
    Assertions.assertEquals(1L, first.fields().get(GlossaryTermEntity.ID));
  }

  private void assertNullArgument(String field, Executable operation) {
    IllegalArgumentException error =
        Assertions.assertThrows(IllegalArgumentException.class, operation);
    Assertions.assertEquals(field + " cannot be null", error.getMessage());
  }
}
