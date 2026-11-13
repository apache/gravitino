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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTermChange {

  @Test
  public void testSingleValueChanges() {
    assertChange(TermChange.rename("Client"), TermChange.rename("Buyer"), "RENAMETERM Client");
    Assertions.assertEquals(
        "Client", ((TermChange.RenameTerm) TermChange.rename("Client")).getNewName());

    assertChange(
        TermChange.updateDefinition("A paying customer"),
        TermChange.updateDefinition("Another definition"),
        "UPDATEDEFINITION A paying customer");
    assertChange(
        TermChange.setStatus(GlossaryTerm.Status.DEPRECATED),
        TermChange.setStatus(GlossaryTerm.Status.DRAFT),
        "SETSTATUS DEPRECATED");
    Assertions.assertEquals(
        GlossaryTerm.Status.DEPRECATED,
        ((TermChange.SetStatus) TermChange.setStatus(GlossaryTerm.Status.DEPRECATED))
            .getNewStatus());

    assertChange(
        TermChange.addSynonym("client"), TermChange.addSynonym("buyer"), "ADDSYNONYM client");
    assertChange(
        TermChange.removeSynonym("client"),
        TermChange.removeSynonym("buyer"),
        "REMOVESYNONYM client");
    assertChange(TermChange.addDomain("Sales"), TermChange.addDomain("Finance"), "ADDDOMAIN Sales");
    assertChange(
        TermChange.removeDomain("Sales"), TermChange.removeDomain("Finance"), "REMOVEDOMAIN Sales");
    assertChange(
        TermChange.addBroader("Customer"), TermChange.addBroader("Party"), "ADDBROADER Customer");
    assertChange(
        TermChange.removeBroader("Customer"),
        TermChange.removeBroader("Party"),
        "REMOVEBROADER Customer");
    assertChange(
        TermChange.addRelated("Order"), TermChange.addRelated("Churn"), "ADDRELATED Order");
    assertChange(
        TermChange.removeRelated("Order"),
        TermChange.removeRelated("Churn"),
        "REMOVERELATED Order");
    assertChange(
        TermChange.setSourceRef("datahub:urn:li:glossaryTerm:Customer"),
        TermChange.setSourceRef("datahub:urn:li:glossaryTerm:Order"),
        "SETSOURCEREF datahub:urn:li:glossaryTerm:Customer");
    assertChange(
        TermChange.removeProperty("owner_team"),
        TermChange.removeProperty("other"),
        "REMOVEPROPERTY owner_team");
  }

  @Test
  public void testChangesOfDifferentTypesAreNotEqual() {
    // The same value in two kinds of change must not compare equal.
    Assertions.assertNotEquals(TermChange.addSynonym("x"), TermChange.removeSynonym("x"));
    Assertions.assertNotEquals(TermChange.addBroader("x"), TermChange.addRelated("x"));
    Assertions.assertNotEquals(TermChange.addDomain("x"), TermChange.addBroader("x"));
  }

  @Test
  public void testNullValues() {
    // A null definition or source reference removes it, and must not break equals or hashCode.
    TermChange clearDefinition = TermChange.updateDefinition(null);
    Assertions.assertNull(((TermChange.UpdateDefinition) clearDefinition).getNewDefinition());
    Assertions.assertEquals(TermChange.updateDefinition(null), clearDefinition);
    Assertions.assertEquals(
        TermChange.updateDefinition(null).hashCode(), clearDefinition.hashCode());
    Assertions.assertNotEquals(TermChange.updateDefinition("x"), clearDefinition);

    TermChange clearSourceRef = TermChange.setSourceRef(null);
    Assertions.assertNull(((TermChange.SetSourceRef) clearSourceRef).getSourceRef());
    Assertions.assertEquals(TermChange.setSourceRef(null), clearSourceRef);
  }

  @Test
  public void testSetProperty() {
    TermChange change = TermChange.setProperty("owner_team", "sales");
    TermChange.SetProperty setProperty = (TermChange.SetProperty) change;
    Assertions.assertEquals("owner_team", setProperty.getProperty());
    Assertions.assertEquals("sales", setProperty.getValue());
    Assertions.assertEquals("SETPROPERTY owner_team sales", change.toString());
    Assertions.assertEquals(TermChange.setProperty("owner_team", "sales"), change);
    Assertions.assertEquals(
        TermChange.setProperty("owner_team", "sales").hashCode(), change.hashCode());
    Assertions.assertNotEquals(TermChange.setProperty("owner_team", "finance"), change);
    Assertions.assertEquals(
        "", ((TermChange.SetProperty) TermChange.setProperty("description", "")).getValue());
  }

  /** Rejects invalid names consistently across all name-bearing change factories. */
  @Test
  public void testInvalidNames() {
    Map<String, Function<String, TermChange>> factories =
        Map.of(
            "rename", TermChange::rename,
            "addSynonym", TermChange::addSynonym,
            "removeSynonym", TermChange::removeSynonym,
            "addDomain", TermChange::addDomain,
            "removeDomain", TermChange::removeDomain,
            "addBroader", TermChange::addBroader,
            "removeBroader", TermChange::removeBroader,
            "addRelated", TermChange::addRelated,
            "removeRelated", TermChange::removeRelated,
            "removeProperty", TermChange::removeProperty);
    for (Map.Entry<String, Function<String, TermChange>> factory : factories.entrySet()) {
      for (String invalid : Arrays.asList(null, "", " ", "\t")) {
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> factory.getValue().apply(invalid),
            factory.getKey() + " must reject invalid name: " + invalid);
      }
    }
  }

  /** Rejects a missing status while preserving every explicit status value. */
  @Test
  public void testNullStatus() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> TermChange.setStatus(null));
    for (GlossaryTerm.Status status : GlossaryTerm.Status.values()) {
      Assertions.assertEquals(
          status, ((TermChange.SetStatus) TermChange.setStatus(status)).getNewStatus());
    }
  }

  /** Rejects malformed property updates at construction. */
  @Test
  public void testInvalidProperties() {
    for (String invalid : Arrays.asList(null, "", " ", "\t")) {
      Assertions.assertThrows(
          IllegalArgumentException.class, () -> TermChange.setProperty(invalid, "value"));
    }
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> TermChange.setProperty("owner_team", null));
  }

  @Test
  public void testUpdateLinks() {
    TermLink[] links = {
      TermLink.of("Wiki", "https://wiki/customer"), TermLink.of(null, "https://x")
    };
    TermChange change = TermChange.updateLinks(links);
    TermChange.UpdateLinks updateLinks = (TermChange.UpdateLinks) change;
    Assertions.assertArrayEquals(links, updateLinks.getLinks());
    Assertions.assertEquals(TermChange.updateLinks(links.clone()), change);
    Assertions.assertEquals(TermChange.updateLinks(links.clone()).hashCode(), change.hashCode());
    Assertions.assertNotEquals(TermChange.updateLinks(new TermLink[0]), change);

    // The change keeps its own copy, so later edits of the caller's array do not change it.
    links[0] = TermLink.of("Other", "https://other");
    updateLinks.getLinks()[1] = TermLink.of("Other", "https://other");
    Assertions.assertEquals(
        List.of(TermLink.of("Wiki", "https://wiki/customer"), TermLink.of(null, "https://x")),
        Arrays.asList(updateLinks.getLinks()));

    Assertions.assertThrows(IllegalArgumentException.class, () -> TermChange.updateLinks(null));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> TermChange.updateLinks(new TermLink[] {TermLink.of("Wiki", "https://x"), null}));
  }

  private static void assertChange(TermChange change, TermChange other, String expectedString) {
    Assertions.assertEquals(expectedString, change.toString());
    Assertions.assertEquals(change, change);
    Assertions.assertNotEquals(other, change);
    Assertions.assertNotEquals(null, change);
  }
}
