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
package org.apache.gravitino;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestStringIdentifier {

  @Test
  public void testCreateStringIdentifierFromId() {
    long uid = 1L;
    StringIdentifier stringId = StringIdentifier.fromId(uid);

    Assertions.assertEquals(uid, stringId.id());

    String expectedIdString =
        String.format(
            StringIdentifier.CURRENT_FORMAT, StringIdentifier.CURRENT_FORMAT_VERSION, uid);
    Assertions.assertEquals(expectedIdString, stringId.toString());
  }

  @Test
  public void testCreateStringIdentifierFromString() {
    long uid = 123123L;
    String idString = "gravitino.v1.uid" + uid;

    StringIdentifier stringId = StringIdentifier.fromString(idString);
    Assertions.assertEquals(uid, stringId.id());
    Assertions.assertEquals(idString, stringId.toString());

    // Test with unsupported version
    String idStringV2 = "gravitino.v2.uid" + uid;
    Throwable ex =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> StringIdentifier.fromString(idStringV2));
    Assertions.assertEquals("Invalid string identifier format: " + idStringV2, ex.getMessage());

    // Test with invalid format
    String idStringInvalid = "gravitino.v1.uid" + uid + ".invalid";
    Throwable ex1 =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> StringIdentifier.fromString(idStringInvalid));
    Assertions.assertEquals(
        "Invalid string identifier format: " + idStringInvalid, ex1.getMessage());

    String idStringInvalid2 = "";
    Throwable ex2 =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> StringIdentifier.fromString(idStringInvalid2));
    Assertions.assertEquals("Input id string cannot be null or empty", ex2.getMessage());

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> StringIdentifier.fromString(null));
    Assertions.assertThrows(IllegalArgumentException.class, () -> StringIdentifier.fromString(" "));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> StringIdentifier.fromString("gravitino.v1.uidfdsafdsa"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> StringIdentifier.fromString("gravitino.v1.uid"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> StringIdentifier.fromString("gravitino.v1.uid1.1"));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> StringIdentifier.fromString("gravitino.v1.uid1.1.1"));
  }

  @Test
  public void testAddStringIdToProperties() {
    long uid = 123123L;
    StringIdentifier stringId = StringIdentifier.fromId(uid);

    Map<String, String> prop = ImmutableMap.of("key1", "value1", "key2", "value2");
    Map<String, String> propWithId = StringIdentifier.newPropertiesWithId(stringId, prop);
    Assertions.assertTrue(propWithId.containsKey(StringIdentifier.ID_KEY));
    Assertions.assertEquals(stringId.toString(), propWithId.get(StringIdentifier.ID_KEY));
    Assertions.assertEquals("value1", propWithId.get("key1"));
    Assertions.assertEquals("value2", propWithId.get("key2"));

    // Test if the input properties is null
    Map<String, String> propWithId1 = StringIdentifier.newPropertiesWithId(stringId, null);
    Assertions.assertTrue(propWithId1.containsKey(StringIdentifier.ID_KEY));
    Assertions.assertEquals(stringId.toString(), propWithId1.get(StringIdentifier.ID_KEY));
    Assertions.assertEquals(1, propWithId1.size());

    // Test if the input properties already have the string identifier
    Map<String, String> prop1 =
        ImmutableMap.of("k1", "v1", StringIdentifier.ID_KEY, stringId.toString());
    StringIdentifier newStringId = StringIdentifier.fromId(12341234L);
    Map<String, String> propWithId2 = StringIdentifier.newPropertiesWithId(newStringId, prop1);
    Assertions.assertEquals(stringId.toString(), propWithId2.get(StringIdentifier.ID_KEY));
  }

  @Test
  public void testGetStringIdFromProperties() {
    long uid = 123123L;
    StringIdentifier stringId = StringIdentifier.fromId(uid);

    Map<String, String> prop = ImmutableMap.of("key1", "value1", "key2", "value2");
    Map<String, String> propWithId = StringIdentifier.newPropertiesWithId(stringId, prop);
    StringIdentifier stringIdFromProp = StringIdentifier.fromProperties(propWithId);
    Assertions.assertEquals(stringId.id(), stringIdFromProp.id());

    // Test if the input properties is null
    StringIdentifier stringIdFromProp1 = StringIdentifier.fromProperties(null);
    Assertions.assertNull(stringIdFromProp1);

    // Test if the input properties does not have the string identifier
    Map<String, String> prop1 = ImmutableMap.of("k1", "v1", "k2", "v2");
    StringIdentifier stringIdFromProp2 = StringIdentifier.fromProperties(prop1);
    Assertions.assertNull(stringIdFromProp2);
  }

  @Test
  public void testAddStringIdToComment() {
    long uid = 123123L;
    StringIdentifier stringId = StringIdentifier.fromId(uid);

    String comment = "This is a comment";
    String commentWithId = StringIdentifier.addToComment(stringId, comment);
    Assertions.assertTrue(
        commentWithId.contains("(From Gravitino, DO NOT EDIT: " + stringId.toString() + ")"));
    Assertions.assertTrue(commentWithId.contains(comment));

    // Test if the input comment is null
    String commentWithId1 = StringIdentifier.addToComment(stringId, null);
    Assertions.assertTrue(
        commentWithId1.equals("(From Gravitino, DO NOT EDIT: " + stringId.toString() + ")"));
  }

  @Test
  public void testGetStringIdFromComment() {
    long uid = 123123L;
    StringIdentifier stringId = StringIdentifier.fromId(uid);

    String comment = "This is a comment";
    String commentWithId = StringIdentifier.addToComment(stringId, comment);
    StringIdentifier stringIdFromComment = StringIdentifier.fromComment(commentWithId);
    Assertions.assertEquals(stringId.id(), stringIdFromComment.id());

    // Test if the input comment is null
    StringIdentifier stringIdFromComment1 = StringIdentifier.fromComment(null);
    Assertions.assertNull(stringIdFromComment1);

    // Test if the input comment does not have the string identifier
    String comment1 = "This is a comment";
    StringIdentifier stringIdFromComment2 = StringIdentifier.fromComment(comment1);
    Assertions.assertNull(stringIdFromComment2);

    // Test comment contains parentheses but not the Gravitino prefix
    String comment2 = "This is a comment (other info)";
    Assertions.assertNull(StringIdentifier.fromComment(comment2));

    // Test comment contains parentheses and Gravitino prefix but not the id
    String comment3 = "This is a comment (From Gravitino, DO NOT EDIT: )";
    Assertions.assertNull(StringIdentifier.fromComment(comment3));

    // Test comment where there is no space between Gravitino prefix and id
    String comment4 = "This is a comment (From Gravitino, DO NOT EDIT:gravitino.v1.uid123123)";
    Assertions.assertNull(StringIdentifier.fromComment(comment4));

    // Test comment with trailing characters after the id
    String commentWithExtra = commentWithId + " extra)";
    StringIdentifier idFromCommentExtra = StringIdentifier.fromComment(commentWithExtra);
    Assertions.assertEquals(stringId.id(), idFromCommentExtra.id());
  }

  @Test
  public void testRemoveIdFromComment() {
    String blankComment = "";
    StringIdentifier identifier = StringIdentifier.fromId(123123L);
    String commentWithoutId = StringIdentifier.removeIdFromComment(blankComment);
    Assertions.assertEquals(blankComment, commentWithoutId);

    String addIdComment = StringIdentifier.addToComment(identifier, blankComment);

    Assertions.assertEquals(blankComment, StringIdentifier.removeIdFromComment(addIdComment));

    String comment = "This is a comment";
    String commentWithId = StringIdentifier.addToComment(identifier, comment);
    Assertions.assertEquals(comment, StringIdentifier.removeIdFromComment(commentWithId));
  }

  @Test
  public void testRemoveIdFromCommentTrimsTrailingSpaces() {
    StringIdentifier identifier = StringIdentifier.fromId(42L);
    String commentWithSpace = "This is a comment ";
    String commentWithId = StringIdentifier.addToComment(identifier, commentWithSpace);
    Assertions.assertEquals(
        commentWithSpace.trim(), StringIdentifier.removeIdFromComment(commentWithId));
  }

  @Test
  public void testAddToCommentAvoidsDuplicateSpace() {
    String commentWithSpace1 = "This is a comment ";
    StringIdentifier identifier1 = StringIdentifier.fromId(1L);
    String commentWithId1 = StringIdentifier.addToComment(identifier1, commentWithSpace1);
    Assertions.assertEquals(
        commentWithSpace1.trim() + " (From Gravitino, DO NOT EDIT: " + identifier1 + ")",
        commentWithId1);

    String commentWithSpace2 = "  This is a comment ";
    StringIdentifier identifier2 = StringIdentifier.fromId(2L);
    String commentWithId2 = StringIdentifier.addToComment(identifier2, commentWithSpace2);
    Assertions.assertEquals(
        commentWithSpace2.trim() + " (From Gravitino, DO NOT EDIT: " + identifier2 + ")",
        commentWithId2);
  }
}
