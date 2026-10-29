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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDomainChange {

  @Test
  public void testRename() {
    DomainChange change = DomainChange.rename("markets");
    Assertions.assertInstanceOf(DomainChange.RenameDomain.class, change);
    Assertions.assertEquals("markets", ((DomainChange.RenameDomain) change).getNewName());
    Assertions.assertEquals("RENAMEDOMAIN markets", change.toString());

    Assertions.assertEquals(DomainChange.rename("markets"), change);
    Assertions.assertEquals(DomainChange.rename("markets").hashCode(), change.hashCode());
    Assertions.assertNotEquals(DomainChange.rename("trading"), change);
  }

  @Test
  public void testUpdateComment() {
    DomainChange change = DomainChange.updateComment("new comment");
    Assertions.assertInstanceOf(DomainChange.UpdateDomainComment.class, change);
    Assertions.assertEquals(
        "new comment", ((DomainChange.UpdateDomainComment) change).getNewComment());
    Assertions.assertEquals("UPDATEDOMAINCOMMENT new comment", change.toString());

    Assertions.assertEquals(DomainChange.updateComment("new comment"), change);
    Assertions.assertEquals(
        DomainChange.updateComment("new comment").hashCode(), change.hashCode());
    Assertions.assertNotEquals(DomainChange.updateComment("other comment"), change);

    // A null comment clears the comment and must not break equals or hashCode.
    DomainChange clearComment = DomainChange.updateComment(null);
    Assertions.assertNull(((DomainChange.UpdateDomainComment) clearComment).getNewComment());
    Assertions.assertEquals(DomainChange.updateComment(null), clearComment);
    Assertions.assertEquals(DomainChange.updateComment(null).hashCode(), clearComment.hashCode());
    Assertions.assertNotEquals(change, clearComment);
  }

  @Test
  public void testSetProperty() {
    DomainChange change = DomainChange.setProperty("owner_team", "finance");
    Assertions.assertInstanceOf(DomainChange.SetProperty.class, change);
    DomainChange.SetProperty setProperty = (DomainChange.SetProperty) change;
    Assertions.assertEquals("owner_team", setProperty.getProperty());
    Assertions.assertEquals("finance", setProperty.getValue());
    Assertions.assertEquals("SETDOMAINPROPERTY owner_team = finance", change.toString());

    Assertions.assertEquals(DomainChange.setProperty("owner_team", "finance"), change);
    Assertions.assertEquals(
        DomainChange.setProperty("owner_team", "finance").hashCode(), change.hashCode());
    Assertions.assertNotEquals(DomainChange.setProperty("owner_team", "hr"), change);
    Assertions.assertNotEquals(DomainChange.setProperty("region", "finance"), change);
  }

  @Test
  public void testRemoveProperty() {
    DomainChange change = DomainChange.removeProperty("owner_team");
    Assertions.assertInstanceOf(DomainChange.RemoveProperty.class, change);
    Assertions.assertEquals("owner_team", ((DomainChange.RemoveProperty) change).getProperty());
    Assertions.assertEquals("REMOVEDOMAINPROPERTY owner_team", change.toString());

    Assertions.assertEquals(DomainChange.removeProperty("owner_team"), change);
    Assertions.assertEquals(
        DomainChange.removeProperty("owner_team").hashCode(), change.hashCode());
    Assertions.assertNotEquals(DomainChange.removeProperty("region"), change);
  }

  @Test
  public void testDifferentChangeTypesAreNotEqual() {
    Assertions.assertNotEquals(DomainChange.rename("a"), DomainChange.updateComment("a"));
    Assertions.assertNotEquals(DomainChange.removeProperty("a"), DomainChange.rename("a"));
  }
}
