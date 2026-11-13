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
import java.util.HashMap;
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
}
