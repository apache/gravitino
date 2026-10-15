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
    Assertions.assertTrue(filter.includeSubdomains());
    Assertions.assertTrue(filter.types().isEmpty());
    Assertions.assertFalse(filter.nameContains().isPresent());
    Assertions.assertTrue(filter.subdomains().isEmpty());

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
    Assertions.assertTrue(filter.includeSubdomains());
    Assertions.assertEquals(
        ImmutableSet.of(MetadataObject.Type.TABLE, MetadataObject.Type.VIEW), filter.types());
    Assertions.assertEquals("trade", filter.nameContains().get());
    Assertions.assertEquals(ImmutableSet.of("trading", "tax"), filter.subdomains());

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
  public void testExcludeSubdomains() {
    DomainMemberFilter filter = DomainMemberFilter.builder().withIncludeSubdomains(false).build();
    Assertions.assertFalse(filter.includeSubdomains());
    Assertions.assertNotEquals(DomainMemberFilter.builder().build(), filter);

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            DomainMemberFilter.builder()
                .withIncludeSubdomains(false)
                .withSubdomains("trading")
                .build());

    // An empty set of sub-domains means no sub-domain restriction.
    Assertions.assertEquals(
        filter, DomainMemberFilter.builder().withIncludeSubdomains(false).withSubdomains().build());
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
