/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

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
}
