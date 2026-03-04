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

package org.apache.gravitino.file;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestFilesetChange {
  @Test
  void testRemovePropertyChange() {
    String property = "property1";
    FilesetChange change = FilesetChange.removeProperty(property);

    Assertions.assertInstanceOf(FilesetChange.RemoveProperty.class, change);
    FilesetChange.RemoveProperty removePropertyChange = (FilesetChange.RemoveProperty) change;
    Assertions.assertEquals(property, removePropertyChange.getProperty());
  }

  @Test
  void testSetPropertyChange() {
    String property = "property1";
    String value = "value1";
    FilesetChange filesetChange = FilesetChange.setProperty(property, value);

    Assertions.assertInstanceOf(FilesetChange.SetProperty.class, filesetChange);
    FilesetChange.SetProperty setPropertyChange = (FilesetChange.SetProperty) filesetChange;
    Assertions.assertEquals(property, setPropertyChange.getProperty());
    Assertions.assertEquals(value, setPropertyChange.getValue());
  }

  @Test
  void testUpdateCommentChange() {
    String comment = "comment1";
    FilesetChange filesetChange = FilesetChange.updateComment(comment);

    Assertions.assertInstanceOf(FilesetChange.UpdateFilesetComment.class, filesetChange);
    FilesetChange.UpdateFilesetComment updateCommentChange =
        (FilesetChange.UpdateFilesetComment) filesetChange;
    Assertions.assertEquals(comment, updateCommentChange.getNewComment());
  }

  @Test
  void testRenameFilesetChange() {
    String name = "name1";
    FilesetChange rename = FilesetChange.rename(name);

    Assertions.assertInstanceOf(FilesetChange.RenameFileset.class, rename);
    FilesetChange.RenameFileset renameChange = (FilesetChange.RenameFileset) rename;
    Assertions.assertEquals(name, renameChange.getNewName());
  }

  @Test
  void testEqualsAndHashCode() {
    // Remove property
    String property = "property1";
    FilesetChange removePropertyChange1 = FilesetChange.removeProperty(property);
    FilesetChange removePropertyChange2 = FilesetChange.removeProperty(property);
    FilesetChange removePropertyChange3 = FilesetChange.removeProperty("property2");

    Assertions.assertEquals(removePropertyChange1, removePropertyChange2);
    Assertions.assertEquals(removePropertyChange1.hashCode(), removePropertyChange2.hashCode());

    Assertions.assertNotEquals(removePropertyChange1, removePropertyChange3);
    Assertions.assertNotEquals(removePropertyChange1.hashCode(), removePropertyChange3.hashCode());

    Assertions.assertNotEquals(removePropertyChange2, removePropertyChange3);
    Assertions.assertNotEquals(removePropertyChange2.hashCode(), removePropertyChange3.hashCode());

    // Rename fileset
    String name = "name1";
    FilesetChange renameChange1 = FilesetChange.rename(name);
    FilesetChange renameChange2 = FilesetChange.rename(name);
    FilesetChange renameChange3 = FilesetChange.rename("name2");

    Assertions.assertEquals(renameChange1, renameChange2);
    Assertions.assertEquals(renameChange1.hashCode(), renameChange2.hashCode());

    Assertions.assertNotEquals(renameChange1, renameChange3);
    Assertions.assertNotEquals(renameChange1.hashCode(), renameChange3.hashCode());

    Assertions.assertNotEquals(renameChange2, renameChange3);
    Assertions.assertNotEquals(renameChange2.hashCode(), renameChange3.hashCode());

    // Update comment
    String comment = "comment1";
    FilesetChange updateCommentChange1 = FilesetChange.updateComment(comment);
    FilesetChange updateCommentChange2 = FilesetChange.updateComment(comment);
    FilesetChange updateCommentChange3 = FilesetChange.updateComment("comment2");

    Assertions.assertEquals(updateCommentChange1, updateCommentChange2);
    Assertions.assertEquals(updateCommentChange1.hashCode(), updateCommentChange2.hashCode());

    Assertions.assertNotEquals(updateCommentChange1, updateCommentChange3);
    Assertions.assertNotEquals(updateCommentChange1.hashCode(), updateCommentChange3.hashCode());

    Assertions.assertNotEquals(updateCommentChange2, updateCommentChange3);
    Assertions.assertNotEquals(updateCommentChange2.hashCode(), updateCommentChange3.hashCode());

    // Set property
    String property2 = "property2";
    String value = "value1";
    FilesetChange setPropertyChange1 = FilesetChange.setProperty(property2, value);
    FilesetChange setPropertyChange2 = FilesetChange.setProperty(property2, value);
    FilesetChange setPropertyChange3 = FilesetChange.setProperty("property3", "value2");

    Assertions.assertEquals(setPropertyChange1, setPropertyChange2);
    Assertions.assertEquals(setPropertyChange1.hashCode(), setPropertyChange2.hashCode());

    Assertions.assertNotEquals(setPropertyChange1, setPropertyChange3);
    Assertions.assertNotEquals(setPropertyChange1.hashCode(), setPropertyChange3.hashCode());

    Assertions.assertNotEquals(setPropertyChange2, setPropertyChange3);
    Assertions.assertNotEquals(setPropertyChange2.hashCode(), setPropertyChange3.hashCode());
  }
}
