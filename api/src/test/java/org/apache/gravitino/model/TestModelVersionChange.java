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

import com.google.common.collect.Lists;
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

  @Test
  void testUpdateUriChangeUseStaticMethod() {
    String newUri = "S3://bucket/key";
    ModelVersionChange modelVersionChange = ModelVersionChange.updateUri(newUri);

    Assertions.assertEquals(ModelVersionChange.UpdateUri.class, modelVersionChange.getClass());

    ModelVersionChange.UpdateUri updateUriChange =
        (ModelVersionChange.UpdateUri) modelVersionChange;
    Assertions.assertEquals(newUri, updateUriChange.newUri());
    Assertions.assertEquals("UpdateUri " + newUri, updateUriChange.toString());
  }

  @Test
  void testUpdateUriChangeUseConstructor() {
    String newUri = "S3://bucket/key";
    ModelVersionChange modelVersionChange = new ModelVersionChange.UpdateUri(newUri);

    Assertions.assertEquals(ModelVersionChange.UpdateUri.class, modelVersionChange.getClass());

    ModelVersionChange.UpdateUri updateUriChange =
        (ModelVersionChange.UpdateUri) modelVersionChange;
    Assertions.assertEquals(newUri, updateUriChange.newUri());
    Assertions.assertEquals("UpdateUri " + newUri, updateUriChange.toString());
  }

  @Test
  void testUpdateUriChangeEquals() {
    String uri1 = "S3://bucket/key1";
    String uri2 = "S3://bucket/key2";

    ModelVersionChange modelVersionChange1 = ModelVersionChange.updateUri(uri1);
    ModelVersionChange modelVersionChange2 = ModelVersionChange.updateUri(uri1);
    ModelVersionChange modelVersionChange3 = ModelVersionChange.updateUri(uri2);

    Assertions.assertEquals(modelVersionChange1, modelVersionChange2);
    Assertions.assertNotEquals(modelVersionChange1, modelVersionChange3);
    Assertions.assertNotEquals(modelVersionChange2, modelVersionChange3);

    Assertions.assertEquals(modelVersionChange1.hashCode(), modelVersionChange2.hashCode());
    Assertions.assertNotEquals(modelVersionChange1.hashCode(), modelVersionChange3.hashCode());
    Assertions.assertNotEquals(modelVersionChange2.hashCode(), modelVersionChange3.hashCode());
  }

  @Test
  void testCreateUpdateVersionAliasUseStaticMethod1() {
    String alias1 = "test";
    String alias2 = "test2";
    ModelVersionChange modelVersionChange = ModelVersionChange.updateAlias(alias1, alias2);

    Assertions.assertEquals(ModelVersionChange.UpdateAlias.class, modelVersionChange.getClass());

    ModelVersionChange.UpdateAlias updateAliasChange =
        (ModelVersionChange.UpdateAlias) modelVersionChange;
    Assertions.assertEquals(Lists.newArrayList(alias1, alias2), updateAliasChange.newAlias());
    Assertions.assertEquals("UpdateAlias " + alias1 + "," + alias2, updateAliasChange.toString());
  }

  @Test
  void testCreateUpdateVersionAliasUseStaticMethod2() {
    String alias1 = "test";
    String alias2 = "test2";

    ModelVersionChange modelVersionChange =
        ModelVersionChange.updateAlias(Lists.newArrayList(alias1, alias2));

    Assertions.assertEquals(ModelVersionChange.UpdateAlias.class, modelVersionChange.getClass());

    ModelVersionChange.UpdateAlias updateAliasChange =
        (ModelVersionChange.UpdateAlias) modelVersionChange;
    Assertions.assertEquals(Lists.newArrayList(alias1, alias2), updateAliasChange.newAlias());
    Assertions.assertEquals("UpdateAlias " + alias1 + "," + alias2, updateAliasChange.toString());
  }

  @Test
  void testCreateUpdateVersionAliasUseConstructor() {
    String alias1 = "test";
    String alias2 = "test2";
    ModelVersionChange modelVersionChange =
        new ModelVersionChange.UpdateAlias(Lists.newArrayList(alias1, alias2));

    Assertions.assertEquals(ModelVersionChange.UpdateAlias.class, modelVersionChange.getClass());

    ModelVersionChange.UpdateAlias updateAliasChange =
        (ModelVersionChange.UpdateAlias) modelVersionChange;
    Assertions.assertEquals(Lists.newArrayList(alias1, alias2), updateAliasChange.newAlias());
    Assertions.assertEquals("UpdateAlias " + alias1 + "," + alias2, updateAliasChange.toString());
  }

  @Test
  void testUpdateVersionAliasChangeEquals() {
    String alias1 = "test1";
    String alias2 = "test2";
    String alias3 = "test3";

    ModelVersionChange modelVersionChange1 = ModelVersionChange.updateAlias(alias1, alias2);
    ModelVersionChange modelVersionChange2 = ModelVersionChange.updateAlias(alias1, alias2);
    ModelVersionChange modelVersionChange3 = ModelVersionChange.updateAlias(alias2, alias3);
    ModelVersionChange modelVersionChange4 = ModelVersionChange.updateAlias(alias2, alias1);

    Assertions.assertEquals(modelVersionChange1, modelVersionChange2);
    Assertions.assertNotEquals(modelVersionChange1, modelVersionChange3);
    Assertions.assertEquals(modelVersionChange1, modelVersionChange4);
    Assertions.assertNotEquals(modelVersionChange2, modelVersionChange3);
    Assertions.assertEquals(modelVersionChange2, modelVersionChange4);
    Assertions.assertNotEquals(modelVersionChange3, modelVersionChange4);

    Assertions.assertEquals(modelVersionChange1.hashCode(), modelVersionChange2.hashCode());
    Assertions.assertNotEquals(modelVersionChange1.hashCode(), modelVersionChange3.hashCode());
    Assertions.assertEquals(modelVersionChange1.hashCode(), modelVersionChange4.hashCode());
    Assertions.assertNotEquals(modelVersionChange2.hashCode(), modelVersionChange3.hashCode());
    Assertions.assertEquals(modelVersionChange2.hashCode(), modelVersionChange4.hashCode());
    Assertions.assertNotEquals(modelVersionChange3.hashCode(), modelVersionChange4.hashCode());
  }
}
