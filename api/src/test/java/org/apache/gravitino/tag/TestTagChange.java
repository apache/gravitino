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

package org.apache.gravitino.tag;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTagChange {

  @Test
  void testUpdateCommentChange() {
    String comment = "comment1";
    TagChange tagChange = TagChange.updateComment(comment);

    Assertions.assertInstanceOf(TagChange.UpdateTagComment.class, tagChange);
    TagChange.UpdateTagComment updateTagComment = (TagChange.UpdateTagComment) tagChange;
    Assertions.assertEquals(comment, updateTagComment.getNewComment());
  }

  @Test
  void testRemovePropertyChange() {
    String property = "property1";
    TagChange tagChange = TagChange.removeProperty(property);

    Assertions.assertInstanceOf(TagChange.RemoveProperty.class, tagChange);
    TagChange.RemoveProperty removeTagProperty = (TagChange.RemoveProperty) tagChange;
    Assertions.assertEquals(property, removeTagProperty.getProperty());
  }

  @Test
  void testRenameChange() {
    String name = "name1";
    TagChange tagChange = TagChange.rename(name);

    Assertions.assertInstanceOf(TagChange.RenameTag.class, tagChange);
    TagChange.RenameTag renameTag = (TagChange.RenameTag) tagChange;
    Assertions.assertEquals(name, renameTag.getNewName());
  }

  @Test
  void testSetPropertyChange() {
    String property = "property1";
    String value = "value1";
    TagChange tagChange = TagChange.setProperty(property, value);

    Assertions.assertInstanceOf(TagChange.SetProperty.class, tagChange);
    TagChange.SetProperty setTagProperty = (TagChange.SetProperty) tagChange;
    Assertions.assertEquals(property, setTagProperty.getProperty());
    Assertions.assertEquals(value, setTagProperty.getValue());
  }

  @Test
  void testEqualsAndHashCode() {
    // Update comment
    TagChange updateComment1 = TagChange.updateComment("comment1");
    TagChange updateComment2 = TagChange.updateComment("comment1");
    TagChange updateComment3 = TagChange.updateComment("comment2");

    Assertions.assertEquals(updateComment1, updateComment2);
    Assertions.assertEquals(updateComment1.hashCode(), updateComment2.hashCode());

    Assertions.assertNotEquals(updateComment1, updateComment3);
    Assertions.assertNotEquals(updateComment1.hashCode(), updateComment3.hashCode());

    Assertions.assertNotEquals(updateComment2, updateComment3);
    Assertions.assertNotEquals(updateComment2.hashCode(), updateComment3.hashCode());

    // Remove property
    TagChange removeProperty1 = TagChange.removeProperty("property1");
    TagChange removeProperty2 = TagChange.removeProperty("property1");
    TagChange removeProperty3 = TagChange.removeProperty("property2");

    Assertions.assertEquals(removeProperty1, removeProperty2);
    Assertions.assertEquals(removeProperty1.hashCode(), removeProperty2.hashCode());

    Assertions.assertNotEquals(removeProperty1, removeProperty3);
    Assertions.assertNotEquals(removeProperty1.hashCode(), removeProperty3.hashCode());

    Assertions.assertNotEquals(removeProperty2, removeProperty3);
    Assertions.assertNotEquals(removeProperty2.hashCode(), removeProperty3.hashCode());

    // Rename
    TagChange rename1 = TagChange.rename("name1");
    TagChange rename2 = TagChange.rename("name1");
    TagChange rename3 = TagChange.rename("name2");

    Assertions.assertEquals(rename1, rename2);
    Assertions.assertEquals(rename1.hashCode(), rename2.hashCode());

    Assertions.assertNotEquals(rename1, rename3);
    Assertions.assertNotEquals(rename1.hashCode(), rename3.hashCode());

    Assertions.assertNotEquals(rename2, rename3);
    Assertions.assertNotEquals(rename2.hashCode(), rename3.hashCode());

    // Set property
    TagChange setProperty1 = TagChange.setProperty("property1", "value1");
    TagChange setProperty2 = TagChange.setProperty("property1", "value1");
    TagChange setProperty3 = TagChange.setProperty("property2", "value2");

    Assertions.assertEquals(setProperty1, setProperty2);
    Assertions.assertEquals(setProperty1.hashCode(), setProperty2.hashCode());

    Assertions.assertNotEquals(setProperty1, setProperty3);
    Assertions.assertNotEquals(setProperty1.hashCode(), setProperty3.hashCode());

    Assertions.assertNotEquals(setProperty2, setProperty3);
    Assertions.assertNotEquals(setProperty2.hashCode(), setProperty3.hashCode());
  }
}
