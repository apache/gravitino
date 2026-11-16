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
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTermValues {

  /** Covers T1.1. */
  @Test
  public void testEnumNamesRoundTrip() {
    for (GlossaryTerm.Status status : GlossaryTerm.Status.values()) {
      Assertions.assertEquals(status, GlossaryTerm.Status.valueOf(status.name()));
    }
    for (TermBinding.Kind kind : TermBinding.Kind.values()) {
      Assertions.assertEquals(kind, TermBinding.Kind.valueOf(kind.name()));
    }
    for (TermMatch.Field field : TermMatch.Field.values()) {
      Assertions.assertEquals(field, TermMatch.Field.valueOf(field.name()));
    }
    Assertions.assertEquals(
        MetadataObject.Type.GLOSSARY_TERM, MetadataObject.Type.valueOf("GLOSSARY_TERM"));
  }

  @Test
  public void testTermLink() {
    TermLink link = TermLink.of("Wiki", "https://wiki/customer");
    Assertions.assertEquals("Wiki", link.label());
    Assertions.assertEquals("https://wiki/customer", link.url());
    Assertions.assertEquals(TermLink.of("Wiki", "https://wiki/customer"), link);
    Assertions.assertEquals(
        TermLink.of("Wiki", "https://wiki/customer").hashCode(), link.hashCode());
    Assertions.assertNotEquals(TermLink.of(null, "https://wiki/customer"), link);
    Assertions.assertNull(TermLink.of(null, "https://x").label());

    Assertions.assertThrows(IllegalArgumentException.class, () -> TermLink.of("Wiki", null));
    Assertions.assertThrows(IllegalArgumentException.class, () -> TermLink.of("Wiki", " "));
  }

  @Test
  public void testTermMatch() {
    TermMatch match = TermMatch.of("active user", TermMatch.Field.SYNONYM);
    Assertions.assertEquals("active user", match.word());
    Assertions.assertEquals(TermMatch.Field.SYNONYM, match.field());
    Assertions.assertEquals(TermMatch.of("active user", TermMatch.Field.SYNONYM), match);
    Assertions.assertNotEquals(TermMatch.of("active user", TermMatch.Field.NAME), match);

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> TermMatch.of("", TermMatch.Field.NAME));
    Assertions.assertThrows(IllegalArgumentException.class, () -> TermMatch.of("x", null));
  }

  @Test
  public void testImplementedByBinding() {
    MetadataObject column =
        MetadataObjects.parse("dw.sales.customers.is_active", MetadataObject.Type.COLUMN);
    TermBinding binding = TermBinding.implementedBy(column);
    Assertions.assertEquals(TermBinding.Kind.IMPLEMENTED_BY, binding.kind());
    Assertions.assertEquals(column, binding.object());
    Assertions.assertFalse(binding.metric().isPresent());
    Assertions.assertEquals(TermBinding.implementedBy(column), binding);
    Assertions.assertEquals(TermBinding.implementedBy(column).hashCode(), binding.hashCode());

    for (MetadataObject.Type type : TermBinding.IMPLEMENTED_BY_TYPES) {
      Assertions.assertEquals(type, TermBinding.implementedBy(objectOf(type)).object().type());
    }
    for (MetadataObject.Type type : MetadataObject.Type.values()) {
      if (!TermBinding.IMPLEMENTED_BY_TYPES.contains(type)) {
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> TermBinding.implementedBy(objectOf(type)),
            "type " + type);
      }
    }
    Assertions.assertThrows(IllegalArgumentException.class, () -> TermBinding.implementedBy(null));
  }

  @Test
  public void testUsedByBinding() {
    MetadataObject model =
        MetadataObjects.parse("dw.sales.sales_model", MetadataObject.Type.SEMANTIC_MODEL);
    TermBinding binding = TermBinding.usedBy(model, "active_customers");
    Assertions.assertEquals(TermBinding.Kind.USED_BY, binding.kind());
    Assertions.assertEquals(model, binding.object());
    Assertions.assertEquals("active_customers", binding.metric().orElse(null));
    Assertions.assertEquals(TermBinding.usedBy(model, "active_customers"), binding);
    Assertions.assertNotEquals(TermBinding.usedBy(model, "net_revenue"), binding);

    MetadataObject table = MetadataObjects.parse("dw.sales.orders", MetadataObject.Type.TABLE);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> TermBinding.usedBy(table, "active_customers"));
    Assertions.assertThrows(IllegalArgumentException.class, () -> TermBinding.usedBy(model, " "));
    Assertions.assertThrows(IllegalArgumentException.class, () -> TermBinding.usedBy(null, "m"));
  }

  /**
   * Mutable targets must not redirect an existing asset binding or invalidate hashed collections.
   */
  @Test
  public void testImplementedBySnapshotsMutableTarget() {
    MutableMetadataObject target =
        new MutableMetadataObject("dw.sales", "orders", MetadataObject.Type.TABLE);
    TermBinding binding = TermBinding.implementedBy(target);
    TermBinding expected =
        TermBinding.implementedBy(
            MetadataObjects.parse("dw.sales.orders", MetadataObject.Type.TABLE));
    assertStableSnapshot(binding, expected, target);
  }

  /** Used-by bindings and pages must also retain the model identity captured at construction. */
  @Test
  public void testUsedBySnapshotsMutableTarget() {
    MutableMetadataObject target =
        new MutableMetadataObject("dw.sales", "sales_model", MetadataObject.Type.SEMANTIC_MODEL);
    TermBinding binding = TermBinding.usedBy(target, "net_revenue");
    TermBinding expected =
        TermBinding.usedBy(
            MetadataObjects.parse("dw.sales.sales_model", MetadataObject.Type.SEMANTIC_MODEL),
            "net_revenue");
    assertStableSnapshot(binding, expected, target);
  }

  /** Snapshot the fields directly: literal dots in a name must not be reparsed as parent levels. */
  @Test
  public void testBindingSnapshotsPreserveIdentityFields() {
    MetadataObject dottedTable =
        MetadataObjects.of(Arrays.asList("dw", "sales", "orders.daily"), MetadataObject.Type.TABLE);
    MetadataObject snapshot = TermBinding.implementedBy(dottedTable).object();
    Assertions.assertEquals("dw.sales", snapshot.parent());
    Assertions.assertEquals("orders.daily", snapshot.name());
    Assertions.assertEquals(MetadataObject.Type.TABLE, snapshot.type());
    MetadataObject catalog =
        TermBinding.implementedBy(MetadataObjects.parse("dw.prod", MetadataObject.Type.CATALOG))
            .object();
    Assertions.assertNull(catalog.parent());
    Assertions.assertEquals("dw.prod", catalog.name());
    Assertions.assertEquals(MetadataObject.Type.CATALOG, catalog.type());
  }

  private static void assertStableSnapshot(
      TermBinding binding, TermBinding expected, MutableMetadataObject target) {
    int originalHash = binding.hashCode();
    Set<TermBinding> hashed = new HashSet<>();
    hashed.add(binding);
    TermBindingPage page = TermBindingPage.of(new TermBinding[] {binding}, "next-page");
    target.parent = "dw.finance";
    target.name = "returns";
    target.type = MetadataObject.Type.GLOSSARY_TERM;
    Assertions.assertEquals(originalHash, binding.hashCode());
    Assertions.assertEquals(expected.kind(), binding.kind());
    Assertions.assertEquals(expected.object().parent(), binding.object().parent());
    Assertions.assertEquals(expected.object().name(), binding.object().name());
    Assertions.assertEquals(expected.object().type(), binding.object().type());
    Assertions.assertEquals(expected.object().fullName(), binding.object().fullName());
    Assertions.assertEquals(expected.metric(), binding.metric());
    Assertions.assertEquals(expected, binding);
    Assertions.assertTrue(hashed.contains(binding));
    Assertions.assertTrue(hashed.contains(expected));
    Assertions.assertEquals(expected, page.bindings()[0]);
    Assertions.assertEquals(originalHash, page.bindings()[0].hashCode());
  }

  private static MetadataObject objectOf(MetadataObject.Type type) {
    switch (type) {
      case METALAKE:
      case CATALOG:
        return MetadataObjects.parse("dw", type);
      case SCHEMA:
        return MetadataObjects.parse("dw.sales", type);
      case COLUMN:
        return MetadataObjects.parse("dw.sales.orders.id", type);
      case FILESET:
      case TABLE:
      case VIEW:
      case SEMANTIC_MODEL:
      case TOPIC:
      case MODEL:
      case FUNCTION:
        return MetadataObjects.parse("dw.sales.orders", type);
      default:
        return MetadataObjects.parse("orders", type);
    }
  }

  private static final class MutableMetadataObject implements MetadataObject {
    @Nullable private String parent;
    private String name;
    private Type type;

    private MutableMetadataObject(@Nullable String parent, String name, Type type) {
      this.parent = parent;
      this.name = name;
      this.type = type;
    }

    @Override
    @Nullable
    public String parent() {
      return parent;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public Type type() {
      return type;
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (!(other instanceof MutableMetadataObject)) {
        return false;
      }
      MutableMetadataObject that = (MutableMetadataObject) other;
      return Objects.equals(parent, that.parent)
          && Objects.equals(name, that.name)
          && type == that.type;
    }

    @Override
    public int hashCode() {
      return Objects.hash(parent, name, type);
    }
  }
}
