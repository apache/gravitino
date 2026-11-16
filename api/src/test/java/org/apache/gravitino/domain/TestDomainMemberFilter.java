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
package org.apache.gravitino.domain;

import com.google.common.collect.ImmutableSet;
import org.apache.gravitino.MetadataObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDomainMemberFilter {

  @Test
  public void testDefaultFilter() {
    DomainMemberFilter filter = DomainMemberFilter.builder().build();
    Assertions.assertTrue(filter.types().isEmpty());
    Assertions.assertFalse(filter.nameContains().isPresent());
    Assertions.assertTrue(filter.domains().isEmpty());

    Assertions.assertEquals(DomainMemberFilter.builder().build(), filter);
    Assertions.assertEquals(DomainMemberFilter.builder().build().hashCode(), filter.hashCode());
  }

  @Test
  public void testFilterWithAllFields() {
    DomainMemberFilter filter =
        DomainMemberFilter.builder()
            .withTypes(MetadataObject.Type.TABLE, MetadataObject.Type.VIEW)
            .withNameContains("trade")
            .withDomains("finance", "tax")
            .build();
    Assertions.assertEquals(
        ImmutableSet.of(MetadataObject.Type.TABLE, MetadataObject.Type.VIEW), filter.types());
    Assertions.assertEquals("trade", filter.nameContains().get());
    Assertions.assertEquals(ImmutableSet.of("finance", "tax"), filter.domains());

    // Order and duplicates do not matter, and the setters are independent of each other.
    DomainMemberFilter sameFilter =
        DomainMemberFilter.builder()
            .withDomains("tax", "finance", "tax")
            .withNameContains("trade")
            .withTypes(MetadataObject.Type.VIEW, MetadataObject.Type.TABLE)
            .build();
    Assertions.assertEquals(sameFilter, filter);
    Assertions.assertEquals(sameFilter.hashCode(), filter.hashCode());
    Assertions.assertNotEquals(DomainMemberFilter.builder().build(), filter);
    Assertions.assertTrue(filter.toString().contains("domains=[finance, tax]"));
  }

  @Test
  public void testDomains() {
    // The scope of the listed domain itself, or of some of its sub-domains.
    DomainMemberFilter financeScope = DomainMemberFilter.builder().withDomains("finance").build();
    Assertions.assertEquals(ImmutableSet.of("finance"), financeScope.domains());
    DomainMemberFilter taxScope = DomainMemberFilter.builder().withDomains("tax").build();
    Assertions.assertEquals(ImmutableSet.of("tax"), taxScope.domains());
    Assertions.assertNotEquals(financeScope, taxScope);

    // No domain given: the whole scope of the listed domain, the same as the default.
    Assertions.assertEquals(
        DomainMemberFilter.builder().build(), DomainMemberFilter.builder().withDomains().build());
  }

  @Test
  public void testLaterCallReplacesValue() {
    DomainMemberFilter filter =
        DomainMemberFilter.builder()
            .withTypes(MetadataObject.Type.TABLE)
            .withTypes(MetadataObject.Type.VIEW)
            .withNameContains("trade")
            .withNameContains("tax")
            .withDomains("trading")
            .withDomains("tax")
            .build();
    Assertions.assertEquals(ImmutableSet.of(MetadataObject.Type.VIEW), filter.types());
    Assertions.assertEquals("tax", filter.nameContains().get());
    Assertions.assertEquals(ImmutableSet.of("tax"), filter.domains());

    // Setting no types or domains, or a null name, resets the option to its default.
    DomainMemberFilter reset =
        DomainMemberFilter.builder()
            .withTypes(MetadataObject.Type.TABLE)
            .withTypes()
            .withNameContains("trade")
            .withNameContains(null)
            .withDomains("tax")
            .withDomains()
            .build();
    Assertions.assertEquals(DomainMemberFilter.builder().build(), reset);
  }

  @Test
  public void testCollectionsAreImmutable() {
    DomainMemberFilter filter =
        DomainMemberFilter.builder()
            .withTypes(MetadataObject.Type.TABLE)
            .withDomains("trading")
            .build();
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> filter.types().add(MetadataObject.Type.VIEW));
    Assertions.assertThrows(UnsupportedOperationException.class, () -> filter.domains().add("tax"));
  }

  @Test
  public void testSupportedTypes() {
    for (MetadataObject.Type type : DomainMember.SUPPORTED_TYPES) {
      Assertions.assertEquals(
          ImmutableSet.of(type), DomainMemberFilter.builder().withTypes(type).build().types());
    }

    for (MetadataObject.Type type : MetadataObject.Type.values()) {
      if (!DomainMember.SUPPORTED_TYPES.contains(type)) {
        IllegalArgumentException e =
            Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> DomainMemberFilter.builder().withTypes(MetadataObject.Type.TABLE, type));
        Assertions.assertTrue(e.getMessage().contains(type.name()));
      }
    }

    // Only leaf objects are members: never catalogs, schemas, columns, functions or domains.
    Assertions.assertEquals(
        ImmutableSet.of(
            MetadataObject.Type.TABLE,
            MetadataObject.Type.VIEW,
            MetadataObject.Type.FILESET,
            MetadataObject.Type.TOPIC,
            MetadataObject.Type.MODEL,
            MetadataObject.Type.SEMANTIC_MODEL),
        DomainMember.SUPPORTED_TYPES);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> DomainMemberFilter.builder().withTypes(MetadataObject.Type.CATALOG));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> DomainMemberFilter.builder().withTypes(MetadataObject.Type.SCHEMA));
  }

  @Test
  public void testInvalidFields() {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> DomainMemberFilter.builder().withTypes((MetadataObject.Type[]) null));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> DomainMemberFilter.builder().withTypes(MetadataObject.Type.TABLE, null));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> DomainMemberFilter.builder().withNameContains(" "));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> DomainMemberFilter.builder().withDomains((String[]) null));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> DomainMemberFilter.builder().withDomains("tax", ""));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> DomainMemberFilter.builder().withDomains("tax", null));
  }
}
