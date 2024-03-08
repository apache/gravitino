/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hive.dyn;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import com.datastrato.gravitino.catalog.hive.dyn.DynFields.UnboundField;
import org.junit.jupiter.api.Test;

public class TestDynFields {

  @Test
  public void testUnboundField() {
    TestObject testObject = new TestObject();
    testObject.setName("John");
    testObject.setAge(30);

    try {
      DynFields.UnboundField<String> nameField =
          DynFields.builder().impl(TestObject.class, "name").buildChecked();

      assertEquals("John", nameField.get(testObject));
      nameField.set(testObject, "Alice");
      assertEquals("Alice", nameField.get(testObject));
    } catch (NoSuchFieldException exception) {
      fail("NoSuchFieldException not expected");
    }
  }

  @Test
  public void testStaticField() {
    try {
      DynFields.StaticField<String> staticField =
          DynFields.builder().impl(TestObject.class, "staticField").buildStaticChecked();

      assertEquals("Hello", staticField.get());
      staticField.set("Hi");
      assertEquals("Hi", staticField.get());
      staticField.set("Hello");
    } catch (NoSuchFieldException exception) {
      fail("NoSuchFieldException not expected");
    }
  }

  @Test
  public void testBoundField() throws NoSuchFieldException {
    TestObject testObject = new TestObject();
    testObject.setName("John");
    testObject.setAge(30);

    DynFields.BoundField<Integer> ageField =
        DynFields.builder().impl(TestObject.class, "age").buildChecked(testObject);

    assertEquals(30, ageField.get());
    ageField.set(40);
    assertEquals(40, ageField.get());
  }

  @Test
  public void testBuildChecked() throws NoSuchFieldException {
    DynFields.Builder builder = DynFields.builder();
    TestObject testObject = new TestObject();
    testObject.setName("John");
    testObject.setAge(30);

    DynFields.BoundField<String> nameField =
        builder.impl(TestObject.class, "name").buildChecked(testObject);
    assertEquals("John", nameField.get());
  }

  @Test
  public void testBuild() {
    DynFields.Builder builder = DynFields.builder();
    TestObject testObject = new TestObject();
    testObject.setName("John");
    testObject.setAge(30);

    DynFields.BoundField<String> nameField =
        builder.impl(TestObject.class, "name").build(testObject);
    assertEquals("John", nameField.get());
  }

  @Test
  public void testBuildStatic() {
    DynFields.Builder builder = DynFields.builder();

    DynFields.StaticField<String> staticField =
        builder.impl(TestObject.class, "staticField").buildStatic();
    String name = staticField.get();
    assertEquals("Hello", name);
  }

  @Test
  public void testDefaultAlwaysNull() throws NoSuchFieldException {
    DynFields.Builder builder = DynFields.builder();
    TestObject testObject = new TestObject();
    testObject.setName("John");
    testObject.setAge(30);

    builder.defaultAlwaysNull();
    UnboundField<Object> nameField = builder.impl(TestObject.class, "lastname").buildChecked();
  }

  @Test
  public void testImplWithStringNames() throws NoSuchFieldException {
    DynFields.Builder builder = DynFields.builder();
    TestObject testObject = new TestObject();
    testObject.setName("John");
    String className = testObject.getClass().getName();
    String fieldName = "name";

    DynFields.BoundField<String> nameField =
        builder.impl(className, fieldName).buildChecked(testObject);
    assertEquals("John", nameField.get());
  }

  @Test
  public void testBind() throws NoSuchFieldException {
    DynFields.Builder builder = DynFields.builder();
    TestObject testObject = new TestObject();
    testObject.setName("John");

    builder.impl(TestObject.class, "name");
    DynFields.UnboundField<String> nameField = builder.buildChecked();
    DynFields.BoundField<String> boundNameField = nameField.bind(testObject);

    String nameValue = boundNameField.get();
    assertEquals("John", nameValue);

    boundNameField.set("Alice");
    assertEquals("Alice", boundNameField.get());
    assertEquals("Alice", testObject.getName());
  }

  @Test
  public void testHiddenImpByClassName() throws NoSuchFieldException {
    DynFields.Builder builder = DynFields.builder();
    TestObject testObject = new TestObject();

    builder.hiddenImpl(TestObject.class, "hidden");
    DynFields.BoundField<String> hiddenField = builder.buildChecked(testObject);
    assertEquals("secret", hiddenField.get());
  }

  @Test
  public void testHiddenImpByString() throws NoSuchFieldException {
    DynFields.Builder builder = DynFields.builder();
    TestObject testObject = new TestObject();
    String className = testObject.getClass().getName();

    builder.hiddenImpl(className, "hidden");
    DynFields.BoundField<String> hiddenField = builder.buildChecked(testObject);
    assertEquals("secret", hiddenField.get());
  }

  public static class TestObject {
    public String name;
    public static String staticField = "Hello";
    public Integer age;
    private String hidden = "secret";

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public Integer getAge() {
      return age;
    }

    public void setAge(Integer age) {
      this.age = age;
    }
  }
}
