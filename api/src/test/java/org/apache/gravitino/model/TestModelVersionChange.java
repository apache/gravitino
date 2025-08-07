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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.util.List;
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
    Assertions.assertEquals(ModelVersion.URI_NAME_UNKNOWN, updateUriChange.uriName());
    Assertions.assertEquals(
        "UpdateUri uriName: (unknown) newUri: (" + newUri + ")", updateUriChange.toString());

    String uriName = "n1";
    modelVersionChange = ModelVersionChange.updateUri(uriName, newUri);

    Assertions.assertEquals(ModelVersionChange.UpdateUri.class, modelVersionChange.getClass());

    updateUriChange = (ModelVersionChange.UpdateUri) modelVersionChange;
    Assertions.assertEquals(newUri, updateUriChange.newUri());
    Assertions.assertEquals(uriName, updateUriChange.uriName());
    Assertions.assertEquals(
        "UpdateUri uriName: (" + uriName + ") newUri: (" + newUri + ")",
        updateUriChange.toString());
  }

  @Test
  void testUpdateUriChangeUseConstructor() {
    String newUri = "S3://bucket/key";
    ModelVersionChange modelVersionChange = new ModelVersionChange.UpdateUri(newUri);

    Assertions.assertEquals(ModelVersionChange.UpdateUri.class, modelVersionChange.getClass());

    ModelVersionChange.UpdateUri updateUriChange =
        (ModelVersionChange.UpdateUri) modelVersionChange;
    Assertions.assertEquals(newUri, updateUriChange.newUri());
    Assertions.assertEquals(ModelVersion.URI_NAME_UNKNOWN, updateUriChange.uriName());
    Assertions.assertEquals(
        "UpdateUri uriName: (unknown) newUri: (" + newUri + ")", updateUriChange.toString());

    String uriName = "n1";
    modelVersionChange = new ModelVersionChange.UpdateUri(uriName, newUri);

    Assertions.assertEquals(ModelVersionChange.UpdateUri.class, modelVersionChange.getClass());

    updateUriChange = (ModelVersionChange.UpdateUri) modelVersionChange;
    Assertions.assertEquals(newUri, updateUriChange.newUri());
    Assertions.assertEquals(uriName, updateUriChange.uriName());
    Assertions.assertEquals(
        "UpdateUri uriName: (" + uriName + ") newUri: (" + newUri + ")",
        updateUriChange.toString());
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

    modelVersionChange1 = ModelVersionChange.updateUri("n1", uri1);
    modelVersionChange2 = ModelVersionChange.updateUri("n1", uri1);
    modelVersionChange3 = ModelVersionChange.updateUri("n2", uri2);

    Assertions.assertEquals(modelVersionChange1, modelVersionChange2);
    Assertions.assertNotEquals(modelVersionChange1, modelVersionChange3);
    Assertions.assertNotEquals(modelVersionChange2, modelVersionChange3);

    Assertions.assertEquals(modelVersionChange1.hashCode(), modelVersionChange2.hashCode());
    Assertions.assertNotEquals(modelVersionChange1.hashCode(), modelVersionChange3.hashCode());
    Assertions.assertNotEquals(modelVersionChange2.hashCode(), modelVersionChange3.hashCode());
  }

  @Test
  void testAddUriChangeUseStaticMethod() {
    String uri = "S3://bucket/key";
    String uriName = "n1";
    ModelVersionChange modelVersionChange = ModelVersionChange.addUri(uriName, uri);

    Assertions.assertEquals(ModelVersionChange.AddUri.class, modelVersionChange.getClass());

    ModelVersionChange.AddUri change = (ModelVersionChange.AddUri) modelVersionChange;
    Assertions.assertEquals(uri, change.uri());
    Assertions.assertEquals(uriName, change.uriName());
    Assertions.assertEquals(
        "AddUri uriName: (" + uriName + ") uri: (" + uri + ")", change.toString());
  }

  @Test
  void testAddUriChangeUseConstructor() {
    String uri = "S3://bucket/key";
    String uriName = "n1";
    ModelVersionChange modelVersionChange = new ModelVersionChange.AddUri(uriName, uri);

    Assertions.assertEquals(ModelVersionChange.AddUri.class, modelVersionChange.getClass());

    ModelVersionChange.AddUri change = (ModelVersionChange.AddUri) modelVersionChange;
    Assertions.assertEquals(uri, change.uri());
    Assertions.assertEquals(uriName, change.uriName());
    Assertions.assertEquals(
        "AddUri uriName: (" + uriName + ") uri: (" + uri + ")", change.toString());
  }

  @Test
  void testAddUriChangeEquals() {
    String uri1 = "S3://bucket/key1";
    String uri2 = "S3://bucket/key2";
    String uriName1 = "n1";
    String uriName2 = "n2";

    ModelVersionChange modelVersionChange1 = ModelVersionChange.addUri(uriName1, uri1);
    ModelVersionChange modelVersionChange2 = ModelVersionChange.addUri(uriName1, uri1);
    ModelVersionChange modelVersionChange3 = ModelVersionChange.addUri(uriName2, uri2);

    Assertions.assertEquals(modelVersionChange1, modelVersionChange2);
    Assertions.assertNotEquals(modelVersionChange1, modelVersionChange3);
    Assertions.assertNotEquals(modelVersionChange2, modelVersionChange3);

    Assertions.assertEquals(modelVersionChange1.hashCode(), modelVersionChange2.hashCode());
    Assertions.assertNotEquals(modelVersionChange1.hashCode(), modelVersionChange3.hashCode());
    Assertions.assertNotEquals(modelVersionChange2.hashCode(), modelVersionChange3.hashCode());
  }

  @Test
  void testRemoveUriChangeUseStaticMethod() {
    String uriName = "n1";
    ModelVersionChange modelVersionChange = ModelVersionChange.removeUri(uriName);

    Assertions.assertEquals(ModelVersionChange.RemoveUri.class, modelVersionChange.getClass());

    ModelVersionChange.RemoveUri change = (ModelVersionChange.RemoveUri) modelVersionChange;
    Assertions.assertEquals(uriName, change.uriName());
    Assertions.assertEquals("RemoveUri uriName: (" + uriName + ")", change.toString());
  }

  @Test
  void testRemoveUriChangeUseConstructor() {
    String uriName = "n1";
    ModelVersionChange modelVersionChange = new ModelVersionChange.RemoveUri(uriName);

    Assertions.assertEquals(ModelVersionChange.RemoveUri.class, modelVersionChange.getClass());

    ModelVersionChange.RemoveUri change = (ModelVersionChange.RemoveUri) modelVersionChange;
    Assertions.assertEquals(uriName, change.uriName());
    Assertions.assertEquals("RemoveUri uriName: (" + uriName + ")", change.toString());
  }

  @Test
  void testRemoveUriChangeEquals() {
    String uriName1 = "n1";
    String uriName2 = "n2";

    ModelVersionChange modelVersionChange1 = ModelVersionChange.removeUri(uriName1);
    ModelVersionChange modelVersionChange2 = ModelVersionChange.removeUri(uriName1);
    ModelVersionChange modelVersionChange3 = ModelVersionChange.removeUri(uriName2);

    Assertions.assertEquals(modelVersionChange1, modelVersionChange2);
    Assertions.assertNotEquals(modelVersionChange1, modelVersionChange3);
    Assertions.assertNotEquals(modelVersionChange2, modelVersionChange3);

    Assertions.assertEquals(modelVersionChange1.hashCode(), modelVersionChange2.hashCode());
    Assertions.assertNotEquals(modelVersionChange1.hashCode(), modelVersionChange3.hashCode());
    Assertions.assertNotEquals(modelVersionChange2.hashCode(), modelVersionChange3.hashCode());
  }

  @Test
  void testCreateUpdateVersionAliasUseStaticMethod() {
    String[] aliasesToAdd = {"alias add 1", "alias add 2"};
    String[] aliasesToRemove = {"alias remove 1", "alias remove 2"};

    ModelVersionChange modelVersionChange =
        ModelVersionChange.updateAliases(aliasesToAdd, aliasesToRemove);

    Assertions.assertEquals(ModelVersionChange.UpdateAliases.class, modelVersionChange.getClass());

    ModelVersionChange.UpdateAliases updateAliasesChange =
        (ModelVersionChange.UpdateAliases) modelVersionChange;
    Assertions.assertEquals(
        ImmutableSet.of("alias add 1", "alias add 2"), updateAliasesChange.aliasesToAdd());
    Assertions.assertEquals(
        ImmutableSet.of("alias remove 1", "alias remove 2"), updateAliasesChange.aliasesToRemove());
    Assertions.assertEquals(
        "UpdateAlias "
            + "AliasToAdd: (alias add 1,alias add 2)"
            + " "
            + "AliasToRemove: (alias "
            + "remove 1,alias remove 2)",
        updateAliasesChange.toString());
  }

  @Test
  void testCreateUpdateVersionAliasUseStaticMethodWithNull() {
    ModelVersionChange modelVersionChange = ModelVersionChange.updateAliases(null, null);
    Assertions.assertEquals(ModelVersionChange.UpdateAliases.class, modelVersionChange.getClass());

    ModelVersionChange.UpdateAliases updateAliasesChange =
        (ModelVersionChange.UpdateAliases) modelVersionChange;
    Assertions.assertEquals(ImmutableSet.of(), updateAliasesChange.aliasesToAdd());
    Assertions.assertEquals(ImmutableSet.of(), updateAliasesChange.aliasesToRemove());
    Assertions.assertEquals(
        "UpdateAlias AliasToAdd: () AliasToRemove: ()", updateAliasesChange.toString());
  }

  @Test
  void testCreateUpdateVersionAliasUseConstructor() {
    List<String> aliasesToAdd = Lists.newArrayList("alias add 1", "alias add 2");
    List<String> aliasesToRemove = Lists.newArrayList("alias remove 1", "alias remove 2");

    ModelVersionChange modelVersionChange =
        new ModelVersionChange.UpdateAliases(aliasesToAdd, aliasesToRemove);

    Assertions.assertEquals(ModelVersionChange.UpdateAliases.class, modelVersionChange.getClass());

    ModelVersionChange.UpdateAliases updateAliasesChange =
        (ModelVersionChange.UpdateAliases) modelVersionChange;
    Assertions.assertEquals(
        ImmutableSet.of("alias add 1", "alias add 2"), updateAliasesChange.aliasesToAdd());
    Assertions.assertEquals(
        ImmutableSet.of("alias remove 1", "alias remove 2"), updateAliasesChange.aliasesToRemove());
    Assertions.assertEquals(
        "UpdateAlias "
            + "AliasToAdd: (alias add 1,alias add 2)"
            + " "
            + "AliasToRemove: (alias "
            + "remove 1,alias remove 2)",
        updateAliasesChange.toString());
  }

  @Test
  void testCreateUpdateVersionAliasUseConstructorWithNull() {
    ModelVersionChange modelVersionChange = new ModelVersionChange.UpdateAliases(null, null);
    Assertions.assertEquals(ModelVersionChange.UpdateAliases.class, modelVersionChange.getClass());

    ModelVersionChange.UpdateAliases updateAliasesChange =
        (ModelVersionChange.UpdateAliases) modelVersionChange;
    Assertions.assertEquals(ImmutableSet.of(), updateAliasesChange.aliasesToAdd());
    Assertions.assertEquals(ImmutableSet.of(), updateAliasesChange.aliasesToRemove());
    Assertions.assertEquals(
        "UpdateAlias AliasToAdd: () AliasToRemove: ()", updateAliasesChange.toString());
  }

  @Test
  void testUpdateVersionAliasChangeEquals() {
    List<String> aliasesToAdd = Lists.newArrayList("alias add 1", "alias add 2");
    List<String> aliasesToRemove = Lists.newArrayList("alias remove 1", "alias remove 2");

    List<String> differentAliasesToAdd = Lists.newArrayList("alias add 1", "alias add 3");
    List<String> differentAliasesToRemove = Lists.newArrayList("alias remove 1", "alias remove 3");

    ModelVersionChange modelVersionChange1 =
        new ModelVersionChange.UpdateAliases(aliasesToAdd, aliasesToRemove);
    ModelVersionChange modelVersionChange2 =
        new ModelVersionChange.UpdateAliases(aliasesToAdd, aliasesToRemove);
    ModelVersionChange modelVersionChange3 =
        new ModelVersionChange.UpdateAliases(differentAliasesToAdd, aliasesToRemove);
    ModelVersionChange modelVersionChange4 =
        new ModelVersionChange.UpdateAliases(aliasesToAdd, differentAliasesToRemove);

    Assertions.assertEquals(modelVersionChange1, modelVersionChange2);
    Assertions.assertNotEquals(modelVersionChange1, modelVersionChange3);
    Assertions.assertNotEquals(modelVersionChange1, modelVersionChange4);
    Assertions.assertNotEquals(modelVersionChange2, modelVersionChange3);
    Assertions.assertNotEquals(modelVersionChange2, modelVersionChange4);
    Assertions.assertNotEquals(modelVersionChange3, modelVersionChange4);

    Assertions.assertEquals(modelVersionChange1.hashCode(), modelVersionChange2.hashCode());
    Assertions.assertNotEquals(modelVersionChange1.hashCode(), modelVersionChange3.hashCode());
    Assertions.assertNotEquals(modelVersionChange1.hashCode(), modelVersionChange4.hashCode());
    Assertions.assertNotEquals(modelVersionChange2.hashCode(), modelVersionChange3.hashCode());
    Assertions.assertNotEquals(modelVersionChange2.hashCode(), modelVersionChange4.hashCode());
  }
}
