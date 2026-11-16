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
package org.apache.gravitino.glossary;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTermSpec {

  @Test
  public void testDefaults() {
    TermSpec spec = TermSpec.builder("Customer").build();
    Assertions.assertEquals("Customer", spec.name());
    Assertions.assertNull(spec.definition());
    Assertions.assertFalse(spec.status().isPresent());
    Assertions.assertFalse(spec.sourceRef().isPresent());
    Assertions.assertEquals(0, spec.synonyms().length);
    Assertions.assertEquals(0, spec.domains().length);
    Assertions.assertEquals(0, spec.broader().length);
    Assertions.assertEquals(0, spec.related().length);
    Assertions.assertEquals(0, spec.links().length);
    Assertions.assertTrue(spec.properties().isEmpty());
  }

  @Test
  public void testAllFields() {
    TermLink link = TermLink.of("Wiki", "https://wiki/active-customer");
    Map<String, String> properties = new HashMap<>();
    properties.put("owner_team", "sales");
    TermSpec spec =
        TermSpec.builder("Active Customer")
            .withDefinition("A customer with an order in the last 90 days")
            .withSynonyms("active user")
            .withStatus(GlossaryTerm.Status.DRAFT)
            .withDomains("Sales")
            .withBroader("Customer")
            .withRelated("Churn")
            .withLinks(link)
            .withSourceRef("datahub:urn:li:glossaryTerm:ActiveCustomer")
            .withProperties(properties)
            .build();

    Assertions.assertEquals("A customer with an order in the last 90 days", spec.definition());
    Assertions.assertArrayEquals(new String[] {"active user"}, spec.synonyms());
    Assertions.assertEquals(GlossaryTerm.Status.DRAFT, spec.status().orElse(null));
    Assertions.assertArrayEquals(new String[] {"Sales"}, spec.domains());
    Assertions.assertArrayEquals(new String[] {"Customer"}, spec.broader());
    Assertions.assertArrayEquals(new String[] {"Churn"}, spec.related());
    Assertions.assertArrayEquals(new TermLink[] {link}, spec.links());
    Assertions.assertEquals(
        "datahub:urn:li:glossaryTerm:ActiveCustomer", spec.sourceRef().orElse(null));
    Assertions.assertEquals(ImmutableMap.of("owner_team", "sales"), spec.properties());

    // The spec keeps its own copies.
    properties.put("other", "x");
    spec.synonyms()[0] = "changed";
    Assertions.assertEquals(ImmutableMap.of("owner_team", "sales"), spec.properties());
    Assertions.assertArrayEquals(new String[] {"active user"}, spec.synonyms());

    TermSpec same =
        TermSpec.builder("Active Customer")
            .withDefinition("A customer with an order in the last 90 days")
            .withSynonyms("active user")
            .withStatus(GlossaryTerm.Status.DRAFT)
            .withDomains("Sales")
            .withBroader("Customer")
            .withRelated("Churn")
            .withLinks(link)
            .withSourceRef("datahub:urn:li:glossaryTerm:ActiveCustomer")
            .withProperties(ImmutableMap.of("owner_team", "sales"))
            .build();
    Assertions.assertEquals(same, spec);
    Assertions.assertEquals(same.hashCode(), spec.hashCode());
    Assertions.assertNotEquals(TermSpec.builder("Active Customer").build(), spec);
  }

  @Test
  public void testInvalidValues() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> TermSpec.builder(null));
    Assertions.assertThrows(IllegalArgumentException.class, () -> TermSpec.builder(" "));

    TermSpec.Builder builder = TermSpec.builder("Customer");
    Assertions.assertThrows(IllegalArgumentException.class, () -> builder.withSynonyms(" "));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> builder.withDomains((String[]) null));
    Assertions.assertThrows(IllegalArgumentException.class, () -> builder.withBroader(""));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> builder.withRelated("Order", null));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> builder.withLinks((TermLink) null));
  }

  /** Creation and mutation reject the same invalid property names, including Unicode whitespace. */
  @Test
  public void testInvalidPropertyNames() {
    for (String invalid : Arrays.asList(null, "", " ", "\t", "\n", "\u2003")) {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () ->
              TermSpec.builder("Customer")
                  .withProperties(Collections.singletonMap(invalid, "value")));
      Assertions.assertThrows(
          IllegalArgumentException.class, () -> TermChange.setProperty(invalid, "value"));
      Assertions.assertThrows(
          IllegalArgumentException.class, () -> TermChange.removeProperty(invalid));
    }
  }

  /** Reject null values while preserving empty values, empty maps and omitted properties. */
  @Test
  public void testPropertyValuesAndOmission() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            TermSpec.builder("Customer")
                .withProperties(Collections.singletonMap("description", null)));
    TermSpec spec =
        TermSpec.builder("Customer")
            .withProperties(Collections.singletonMap("description", ""))
            .build();
    Assertions.assertEquals("", spec.properties().get("description"));
    Assertions.assertTrue(
        TermSpec.builder("Customer")
            .withProperties(Collections.emptyMap())
            .build()
            .properties()
            .isEmpty());
    TermSpec.Builder builder =
        TermSpec.builder("Customer").withProperties(Collections.singletonMap("description", ""));
    TermSpec before = builder.build();
    Assertions.assertTrue(builder.withProperties(null).build().properties().isEmpty());
    Assertions.assertEquals("", before.properties().get("description"));
  }

  /** Copy caller maps and preserve exact valid names so the same keys can be changed or removed. */
  @Test
  public void testPropertiesAreCopiedWithoutNormalization() {
    Map<String, String> source = new HashMap<>();
    source.put(" display name ", "");
    TermSpec spec = TermSpec.builder("Customer").withProperties(source).build();
    source.put(" display name ", "changed");
    Assertions.assertEquals(Collections.singletonMap(" display name ", ""), spec.properties());
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> spec.properties().put("other", "value"));
    Assertions.assertEquals(
        " display name ",
        ((TermChange.SetProperty) TermChange.setProperty(" display name ", "")).getProperty());
    Assertions.assertEquals(
        " display name ",
        ((TermChange.RemoveProperty) TermChange.removeProperty(" display name ")).getProperty());
  }

  /** A rejected replacement must not publish the valid prefix or clear the previous properties. */
  @Test
  public void testInvalidPropertiesLeaveBuilderUnchanged() {
    TermSpec.Builder builder =
        TermSpec.builder("Customer").withProperties(Collections.singletonMap("existing", "value"));
    Map<String, String> replacement = new LinkedHashMap<>();
    replacement.put("valid", "new");
    replacement.put(" ", "invalid");
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> builder.withProperties(replacement));
    Assertions.assertEquals(
        Collections.singletonMap("existing", "value"), builder.build().properties());
    builder.withProperties(Collections.singletonMap("next", ""));
    Assertions.assertEquals(Collections.singletonMap("next", ""), builder.build().properties());
  }
}
