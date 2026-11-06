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

import org.apache.gravitino.Namespace;
import org.apache.gravitino.tag.TagAssignment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTagEntity {

  /** The stored visibility flag defaults to false and survives assignment/inheritance copies. */
  @Test
  public void testHiddenFlagIsAnEntityField() {
    TagEntity visible =
        TagEntity.builder()
            .withId(1L)
            .withName("tag")
            .withNamespace(Namespace.of("metalake"))
            .withAuditInfo(AuditInfo.EMPTY)
            .build();
    TagEntity hidden =
        TagEntity.builder()
            .withId(1L)
            .withName("tag")
            .withNamespace(Namespace.of("metalake"))
            .withAuditInfo(AuditInfo.EMPTY)
            .withHidden(true)
            .build();
    Assertions.assertFalse(visible.hidden());
    Assertions.assertEquals(false, visible.fields().get(TagEntity.HIDDEN));
    Assertions.assertEquals(true, hidden.fields().get(TagEntity.HIDDEN));
    Assertions.assertNotEquals(visible, hidden);
    Assertions.assertTrue(hidden.copyWithAssignment(TagAssignment.noValue()).hidden());
    Assertions.assertTrue(hidden.copyWithInherited(true).hidden());
  }

  @Test
  public void testTagEntityCarriesAssignmentContextOutsideFields() {
    TagEntity tagEntity =
        TagEntity.builder()
            .withId(1L)
            .withName("tag")
            .withNamespace(Namespace.of("metalake"))
            .withComment("comment")
            .withAuditInfo(AuditInfo.EMPTY)
            .withAssignment(TagAssignment.ofValues("dev", "prod"))
            .build();

    Assertions.assertTrue(tagEntity.assignment().isPresent());
    Assertions.assertArrayEquals(
        new String[] {"dev", "prod"}, tagEntity.assignment().get().values());
    Assertions.assertFalse(tagEntity.fields().values().contains(tagEntity.assignment().get()));
  }

  @Test
  public void testTagEntityEqualityIgnoresAssignmentContext() {
    TagEntity tagEntity =
        TagEntity.builder()
            .withId(1L)
            .withName("tag")
            .withNamespace(Namespace.of("metalake"))
            .withComment("comment")
            .withAuditInfo(AuditInfo.EMPTY)
            .build();

    TagEntity tagEntityWithAssignment =
        tagEntity.copyWithAssignment(TagAssignment.ofValues("dev", "prod"));

    Assertions.assertEquals(tagEntity, tagEntityWithAssignment);
    Assertions.assertEquals(tagEntity.hashCode(), tagEntityWithAssignment.hashCode());
  }
}
