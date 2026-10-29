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

import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDomainRelation {

  @Test
  public void testDirect() {
    DomainRelation relation = DomainRelation.direct();
    Assertions.assertEquals(DomainRelation.Type.DIRECT, relation.type());
    Assertions.assertFalse(relation.source().isPresent());
    Assertions.assertEquals("DIRECT", relation.toString());
    Assertions.assertEquals(DomainRelation.direct(), relation);
    Assertions.assertEquals(DomainRelation.direct().hashCode(), relation.hashCode());
  }

  @Test
  public void testInherited() {
    MetadataObject schema = MetadataObjects.of("cat", "db", MetadataObject.Type.SCHEMA);
    DomainRelation relation = DomainRelation.inherited(schema);
    Assertions.assertEquals(DomainRelation.Type.INHERITED, relation.type());
    Assertions.assertEquals(schema, relation.source().get());
    Assertions.assertEquals("INHERITED cat.db", relation.toString());

    MetadataObject catalog = MetadataObjects.of(null, "cat", MetadataObject.Type.CATALOG);
    Assertions.assertEquals(catalog, DomainRelation.inherited(catalog).source().get());

    Assertions.assertEquals(DomainRelation.inherited(schema), relation);
    Assertions.assertEquals(DomainRelation.inherited(schema).hashCode(), relation.hashCode());
    Assertions.assertNotEquals(DomainRelation.inherited(catalog), relation);
    Assertions.assertNotEquals(DomainRelation.direct(), relation);
  }

  @Test
  public void testInheritedNormalizesSource() {
    // A source from another MetadataObject implementation, such as a DTO, keeps only its name and
    // type, so relations built from it compare equal to those built from MetadataObjects.
    MetadataObject other =
        new MetadataObject() {
          @Override
          public String parent() {
            return "cat";
          }

          @Override
          public String name() {
            return "db";
          }

          @Override
          public MetadataObject.Type type() {
            return MetadataObject.Type.SCHEMA;
          }
        };
    MetadataObject schema = MetadataObjects.of("cat", "db", MetadataObject.Type.SCHEMA);
    DomainRelation relation = DomainRelation.inherited(other);
    Assertions.assertEquals(schema, relation.source().get());
    Assertions.assertEquals(DomainRelation.inherited(schema), relation);
    Assertions.assertEquals(DomainRelation.inherited(schema).hashCode(), relation.hashCode());
  }

  @Test
  public void testInheritedNeedsCatalogOrSchema() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> DomainRelation.inherited(null));
    MetadataObject table = MetadataObjects.of("cat.db", "trades", MetadataObject.Type.TABLE);
    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> DomainRelation.inherited(table));
    Assertions.assertTrue(e.getMessage().contains("TABLE"));
  }

  @Test
  public void testViaSubdomain() {
    DomainRelation relation = DomainRelation.viaSubdomain("trading");
    Assertions.assertEquals(DomainRelation.Type.VIA_SUBDOMAIN, relation.type());
    Assertions.assertEquals(
        MetadataObjects.of(null, "trading", MetadataObject.Type.DOMAIN), relation.source().get());
    Assertions.assertEquals("VIA_SUBDOMAIN trading", relation.toString());

    Assertions.assertEquals(DomainRelation.viaSubdomain("trading"), relation);
    Assertions.assertEquals(DomainRelation.viaSubdomain("trading").hashCode(), relation.hashCode());
    Assertions.assertNotEquals(DomainRelation.viaSubdomain("tax"), relation);

    Assertions.assertThrows(IllegalArgumentException.class, () -> DomainRelation.viaSubdomain(""));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> DomainRelation.viaSubdomain(null));
  }
}
