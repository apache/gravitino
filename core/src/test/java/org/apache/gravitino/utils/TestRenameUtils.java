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
package org.apache.gravitino.utils;

import org.apache.gravitino.tag.TagChange;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class TestRenameUtils {

  @Test
  void testEmptyChangesKeepOriginalName() {
    Assertions.assertEquals(
        "original",
        RenameUtils.getLastRenameName(
            "original",
            new TagChange[0],
            TagChange.RenameTag.class,
            TagChange.RenameTag::getNewName));
  }

  @Test
  void testNonRenameChangesKeepOriginalName() {
    TagChange[] changes = {
      TagChange.updateComment("comment"), TagChange.setProperty("key", "value")
    };
    Assertions.assertEquals(
        "original",
        RenameUtils.getLastRenameName(
            "original", changes, TagChange.RenameTag.class, TagChange.RenameTag::getNewName));
  }

  @Test
  void testLastRenameWinsWithOtherChanges() {
    TagChange[] changes = {
      TagChange.updateComment("comment"),
      TagChange.rename("first"),
      TagChange.setProperty("key", "value"),
      TagChange.rename("last"),
      TagChange.removeProperty("key")
    };
    Assertions.assertEquals(
        "last",
        RenameUtils.getLastRenameName(
            "original", changes, TagChange.RenameTag.class, TagChange.RenameTag::getNewName));
  }
}
