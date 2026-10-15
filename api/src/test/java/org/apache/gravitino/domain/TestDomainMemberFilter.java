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
    Assertions.assertTrue(filter.includeAllSubdomains());
    Assertions.assertTrue(filter.subdomains().isEmpty());
    Assertions.assertTrue(filter.types().isEmpty());
    Assertions.assertFalse(filter.nameContains().isPresent());

    Assertions.assertEquals(DomainMemberFilter.builder().build(), filter);
    Assertions.assertEquals(DomainMemberFilter.builder().build().hashCode(), filter.hashCode());
  }

  @Test
  public void testFilterWithAllFields() {
    DomainMemberFilter filter =
        DomainMemberFilter.builder()
            .withTypes(MetadataObject.Type.TABLE, MetadataObject.Type.VIEW)
            .withNameContains("trade")
            .withSubdomains("trading", "tax")
            .build();
    Assertions.assertFalse(filter.includeAllSubdomains());
    Assertions.assertEquals(ImmutableSet.of("trading", "tax"), filter.subdomains());
    Assertions.assertEquals(
        ImmutableSet.of(MetadataObject.Type.TABLE, MetadataObject.Type.VIEW), filter.types());
    Assertions.assertEquals("trade", filter.nameContains().get());

    DomainMemberFilter sameFilter =
        DomainMemberFilter.builder()
            .withTypes(MetadataObject.Type.VIEW, MetadataObject.Type.TABLE)
            .withNameContains("trade")
            .withSubdomains("tax", "trading", "tax")
            .build();
    Assertions.assertEquals(sameFilter, filter);
    Assertions.assertEquals(sameFilter.hashCode(), filter.hashCode());
    Assertions.assertNotEquals(DomainMemberFilter.builder().build(), filter);
    Assertions.assertTrue(filter.toString().contains("subdomains=[trading, tax]"));
  }

  @Test
  public void testSubdomainOptions() {
    // Only the domain itself.
    DomainMemberFilter ownOnly =
        DomainMemberFilter.builder().withIncludeAllSubdomains(false).build();
    Assertions.assertFalse(ownOnly.includeAllSubdomains());
    Assertions.assertTrue(ownOnly.subdomains().isEmpty());
    Assertions.assertNotEquals(DomainMemberFilter.builder().build(), ownOnly);
    Assertions.assertEquals(ownOnly, DomainMemberFilter.builder().withSubdomains().build());

    // The domain itself plus the given sub-domains, with or without the flag set to false.
    DomainMemberFilter withTax = DomainMemberFilter.builder().withSubdomains("tax").build();
    Assertions.assertFalse(withTax.includeAllSubdomains());
    Assertions.assertEquals(ImmutableSet.of("tax"), withTax.subdomains());
    Assertions.assertEquals(
        withTax,
        DomainMemberFilter.builder().withIncludeAllSubdomains(false).withSubdomains("tax").build());
    Assertions.assertEquals(
        withTax,
        DomainMemberFilter.builder().withSubdomains("tax").withIncludeAllSubdomains(false).build());

    // The last call wins: including all sub-domains clears the given ones, and vice versa.
    DomainMemberFilter all =
        DomainMemberFilter.builder().withSubdomains("tax").withIncludeAllSubdomains(true).build();
    Assertions.assertTrue(all.includeAllSubdomains());
    Assertions.assertTrue(all.subdomains().isEmpty());
    Assertions.assertEquals(DomainMemberFilter.builder().build(), all);
    Assertions.assertEquals(
        withTax,
        DomainMemberFilter.builder().withIncludeAllSubdomains(true).withSubdomains("tax").build());
  }

  @Test
  public void testCollectionsAreImmutable() {
    DomainMemberFilter filter =
        DomainMemberFilter.builder()
            .withTypes(MetadataObject.Type.TABLE)
            .withSubdomains("trading")
            .build();
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> filter.types().add(MetadataObject.Type.VIEW));
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> filter.subdomains().add("tax"));
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

    Assertions.assertFalse(DomainMember.SUPPORTED_TYPES.contains(MetadataObject.Type.DOMAIN));
    Assertions.assertFalse(DomainMember.SUPPORTED_TYPES.contains(MetadataObject.Type.COLUMN));
    Assertions.assertFalse(DomainMember.SUPPORTED_TYPES.contains(MetadataObject.Type.FUNCTION));
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
        () -> DomainMemberFilter.builder().withSubdomains((String[]) null));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> DomainMemberFilter.builder().withSubdomains("trading", ""));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> DomainMemberFilter.builder().withSubdomains("trading", null));

    // Null clears the name filter.
    DomainMemberFilter filter = DomainMemberFilter.builder().withNameContains(null).build();
    Assertions.assertEquals(DomainMemberFilter.builder().build(), filter);
  }
}
