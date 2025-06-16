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

public class TestModelVersionChange {
  @Test
  void testCreateUpdateVersionCommentChangeUseStaticMethod() {
    String newComment = "new comment";
    ModelVersionChange modelVersionChange = ModelVersionChange.updateComment(newComment);
    Assertions.assertEquals(ModelVersionChange.UpdateComment.class, modelVersionChange.getClass());

    ModelVersionChange.UpdateComment updateCommentChange =
        (ModelVersionChange.UpdateComment) modelVersionChange;
    Assertions.assertEquals(newComment, updateCommentChange.newComment());
    Assertions.assertEquals("UpdateComment " + newComment, updateCommentChange.toString());
  }

  @Test
  void testCreateUpdateVersionCommentChangeUseConstructor() {
    String newComment = "new comment";
    ModelVersionChange modelVersionChange = new ModelVersionChange.UpdateComment(newComment);
    Assertions.assertEquals(ModelVersionChange.UpdateComment.class, modelVersionChange.getClass());

    ModelVersionChange.UpdateComment updateCommentChange =
        (ModelVersionChange.UpdateComment) modelVersionChange;
    Assertions.assertEquals(newComment, updateCommentChange.newComment());
    Assertions.assertEquals("UpdateComment " + newComment, updateCommentChange.toString());
  }

  @Test
  void testUpdateVersionCommentChangeEquals() {
    String newComment = "new comment";
    String differentComment = "different comment";
    ModelVersionChange modelVersionChange1 = ModelVersionChange.updateComment(newComment);
    ModelVersionChange modelVersionChange2 = ModelVersionChange.updateComment(newComment);
    ModelVersionChange modelVersionChange3 = ModelVersionChange.updateComment(differentComment);

    Assertions.assertEquals(modelVersionChange1, modelVersionChange2);
    Assertions.assertNotEquals(modelVersionChange1, modelVersionChange3);
    Assertions.assertNotEquals(modelVersionChange2, modelVersionChange3);

    Assertions.assertEquals(modelVersionChange1.hashCode(), modelVersionChange2.hashCode());
    Assertions.assertNotEquals(modelVersionChange1.hashCode(), modelVersionChange3.hashCode());
    Assertions.assertNotEquals(modelVersionChange2.hashCode(), modelVersionChange3.hashCode());
  }

  @Test
  void testCreateSetPropertyChangeUseStaticMethod() {
    String property = "property";
    String value = "value";
    ModelVersionChange modelVersionChange = ModelVersionChange.setProperty(property, value);

    Assertions.assertEquals(ModelVersionChange.SetProperty.class, modelVersionChange.getClass());

    ModelVersionChange.SetProperty setPropertyChange =
        (ModelVersionChange.SetProperty) modelVersionChange;
    Assertions.assertEquals(property, setPropertyChange.property());
    Assertions.assertEquals(value, setPropertyChange.value());
    Assertions.assertEquals("SETPROPERTY " + property + " " + value, setPropertyChange.toString());
  }

  @Test
  void testCreateSetPropertyChangeUseConstructor() {
    String property = "property";
    String value = "value";
    ModelVersionChange modelVersionChange = new ModelVersionChange.SetProperty(property, value);

    Assertions.assertEquals(ModelVersionChange.SetProperty.class, modelVersionChange.getClass());

    ModelVersionChange.SetProperty setPropertyChange =
        (ModelVersionChange.SetProperty) modelVersionChange;
    Assertions.assertEquals(property, setPropertyChange.property());
    Assertions.assertEquals(value, setPropertyChange.value());
    Assertions.assertEquals("SETPROPERTY " + property + " " + value, setPropertyChange.toString());
  }

  @Test
  void testSetPropertyChangeEquals() {
    String property1 = "property1";
    String value1 = "value1";
    String property2 = "property2";
    ModelVersionChange modelVersionChange1 = ModelVersionChange.setProperty(property1, value1);
    ModelVersionChange modelVersionChange2 = ModelVersionChange.setProperty(property1, value1);
    ModelVersionChange modelVersionChange3 = ModelVersionChange.setProperty(property2, value1);

    Assertions.assertEquals(modelVersionChange1, modelVersionChange2);
    Assertions.assertNotEquals(modelVersionChange1, modelVersionChange3);
    Assertions.assertNotEquals(modelVersionChange2, modelVersionChange3);

    Assertions.assertEquals(modelVersionChange1.hashCode(), modelVersionChange2.hashCode());
    Assertions.assertNotEquals(modelVersionChange1.hashCode(), modelVersionChange3.hashCode());
    Assertions.assertNotEquals(modelVersionChange2.hashCode(), modelVersionChange3.hashCode());
  }

  @Test
  void testCreateRemovePropertyChangeUseStaticMethod() {
    String property = "property";
    ModelVersionChange modelVersionChange = ModelVersionChange.removeProperty(property);

    Assertions.assertEquals(ModelVersionChange.RemoveProperty.class, modelVersionChange.getClass());

    ModelVersionChange.RemoveProperty removePropertyChange =
        (ModelVersionChange.RemoveProperty) modelVersionChange;
    Assertions.assertEquals(property, removePropertyChange.property());
    Assertions.assertEquals("REMOVEPROPERTY " + property, removePropertyChange.toString());
  }

  @Test
  void testCreateRemovePropertyChangeUseConstructor() {
    String property = "property";
    ModelVersionChange modelVersionChange = new ModelVersionChange.RemoveProperty(property);

    Assertions.assertEquals(ModelVersionChange.RemoveProperty.class, modelVersionChange.getClass());

    ModelVersionChange.RemoveProperty removePropertyChange =
        (ModelVersionChange.RemoveProperty) modelVersionChange;
    Assertions.assertEquals(property, removePropertyChange.property());
    Assertions.assertEquals("REMOVEPROPERTY " + property, removePropertyChange.toString());
  }

  @Test
  void testRemovePropertyChangeEquals() {
    String property1 = "property1";
    String property2 = "property2";

    ModelVersionChange modelVersionChange1 = ModelVersionChange.removeProperty(property1);
    ModelVersionChange modelVersionChange2 = ModelVersionChange.removeProperty(property1);
    ModelVersionChange modelVersionChange3 = ModelVersionChange.removeProperty(property2);

    Assertions.assertEquals(modelVersionChange1, modelVersionChange2);
    Assertions.assertNotEquals(modelVersionChange1, modelVersionChange3);
    Assertions.assertNotEquals(modelVersionChange2, modelVersionChange3);

    Assertions.assertEquals(modelVersionChange1.hashCode(), modelVersionChange2.hashCode());
    Assertions.assertNotEquals(modelVersionChange1.hashCode(), modelVersionChange3.hashCode());
    Assertions.assertNotEquals(modelVersionChange2.hashCode(), modelVersionChange3.hashCode());
  }
}
