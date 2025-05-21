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

package org.apache.gravitino.model;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestModelChange {

  @Test
  void testCreateModelChangeUseStaticMethod() {
    String newName = "newName";
    ModelChange modelChange = ModelChange.rename(newName);
    Assertions.assertEquals(ModelChange.RenameModel.class, modelChange.getClass());

    ModelChange.RenameModel renameModel = (ModelChange.RenameModel) modelChange;
    Assertions.assertEquals(newName, renameModel.newName());
    Assertions.assertEquals("RenameModel newName", renameModel.toString());
  }

  @Test
  void testCreateModelChangeUseConstructor() {
    String newName = "newName";
    ModelChange.RenameModel renameModel = new ModelChange.RenameModel(newName);
    Assertions.assertEquals(newName, renameModel.newName());
    Assertions.assertEquals("RenameModel newName", renameModel.toString());
  }

  @Test
  void testModelChangeEquals() {
    String newName1 = "demo_model";
    String newName2 = "test_model";
    String newName3 = "demo_model";

    ModelChange.RenameModel renameModel1 = new ModelChange.RenameModel(newName1);
    ModelChange.RenameModel renameModel2 = new ModelChange.RenameModel(newName2);
    ModelChange.RenameModel renameModel3 = new ModelChange.RenameModel(newName3);

    Assertions.assertEquals(renameModel1, renameModel3);
    Assertions.assertNotEquals(renameModel1, renameModel2);
    Assertions.assertEquals(renameModel1, renameModel3);

    Assertions.assertNotEquals(renameModel1.hashCode(), renameModel2.hashCode());
    Assertions.assertEquals(renameModel1.hashCode(), renameModel3.hashCode());
  }

  @Test
  void testUpdateModelCommentWithStaticMethod() {
    String newComment = "newComment";
    ModelChange modelChange = ModelChange.updateComment(newComment);
    Assertions.assertEquals(ModelChange.UpdateComment.class, modelChange.getClass());

    ModelChange.UpdateComment updateComment = (ModelChange.UpdateComment) modelChange;
    Assertions.assertEquals(newComment, updateComment.newComment());
    Assertions.assertEquals("UpdateComment newComment", updateComment.toString());
  }

  @Test
  void testUpdateModelCommentWithConstructor() {
    String newComment = "newComment";
    ModelChange.UpdateComment updateComment = new ModelChange.UpdateComment(newComment);
    Assertions.assertEquals(newComment, updateComment.newComment());
    Assertions.assertEquals("UpdateComment newComment", updateComment.toString());
  }

  @Test
  void testUpdateModelCommentEquals() {
    String newComment1 = "This is a demo model";
    String newComment2 = "This is a test model";
    String newComment3 = "This is a demo model";

    ModelChange.UpdateComment updateComment1 = new ModelChange.UpdateComment(newComment1);
    ModelChange.UpdateComment updateComment2 = new ModelChange.UpdateComment(newComment2);
    ModelChange.UpdateComment updateComment3 = new ModelChange.UpdateComment(newComment3);

    Assertions.assertEquals(updateComment1, updateComment3);
    Assertions.assertNotEquals(updateComment1, updateComment2);
    Assertions.assertEquals(updateComment1, updateComment3);

    Assertions.assertNotEquals(updateComment1.hashCode(), updateComment2.hashCode());
    Assertions.assertEquals(updateComment1.hashCode(), updateComment3.hashCode());
    Assertions.assertEquals(updateComment1.toString(), updateComment3.toString());
  }
}
