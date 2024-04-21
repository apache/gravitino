/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

/** The helper class for {@link SecurableObject}. */
public class SecurableObjects {

  private static final Splitter DOT = Splitter.on('.');

  /**
   * Create the {@link SecurableObject} with the given names.
   *
   * @param type The securable object type.
   * @param names The names of the securable object.
   * @return The created {@link SecurableObject}
   */
  public static SecurableObject of(SecurableObject.Type type, String... names) {

    if (names == null) {
      throw new IllegalArgumentException("Cannot create a securable object with null names");
    }

    if (names.length == 0) {
      throw new IllegalArgumentException("Cannot create a securable object with no names");
    }

    if (type == null) {
      throw new IllegalArgumentException("Cannot create a securable object with no type");
    }

    if (names.length > 3) {
      throw new IllegalArgumentException(
          "Cannot create a securable object with the name length which is greater than 3");
    }

    if (names.length == 1
        && type != SecurableObject.Type.CATALOG
        && type != SecurableObject.Type.METALAKE) {
      throw new IllegalArgumentException(
          "If the length of names is 1, it must be the CATALOG or METALAKE type");
    }

    if (names.length == 2 && type != SecurableObject.Type.SCHEMA) {
      throw new IllegalArgumentException("If the length of names is 2, it must be the SCHEMA type");
    }

    if (names.length == 3
        && type != SecurableObject.Type.FILESET
        && type != SecurableObject.Type.TABLE
        && type != SecurableObject.Type.TOPIC) {
      throw new IllegalArgumentException(
          "If the length of names is 3, it must be FILESET, TABLE or TOPIC");
    }

    List<SecurableObject.Type> types = Lists.newArrayList(type);

    // Find all the types of the parent securable object.
    SecurableObject.Type curType = type;
    for (int parentNum = names.length - 2; parentNum >= 0; parentNum--) {
      curType = getParentSecurableObjectType(curType);
      types.add(curType);
    }
    Collections.reverse(types);

    SecurableObject parent = null;
    int level = 0;
    for (String name : names) {
      checkName(name);

      if (name.equals("*")) {
        throw new IllegalArgumentException(
            "Cannot create a securable object with `*` name. If you want to use a securable object which represents all metalakes,"
                + " you should use the method `ofAllMetalakes`. If you want to use a securable object which represents all catalogs,"
                + " you should use the method `ofMetalake`."
                + " If you want to create an another securable object which represents all entities,"
                + " you can use its parent entity, For example,"
                + " if you want to have read table privileges of all tables of `catalog1.schema1`,"
                + " you can use add `read table` privilege for `catalog1.schema1` directly");
      }

      parent = new SecurableObjectImpl(parent, name, types.get(level));

      level++;
    }

    return parent;
  }

  /**
   * Create the metalake {@link SecurableObject} with the given metalake name.
   *
   * @param metalake The metalake name
   * @return The created metalake {@link SecurableObject}
   */
  public static SecurableObject ofMetalake(String metalake) {
    checkName(metalake);

    return new SecurableObjectImpl(null, metalake, SecurableObject.Type.METALAKE);
  }

  /**
   * Create the catalog {@link SecurableObject} with the given catalog name.
   *
   * @param catalog The catalog name
   * @return The created catalog {@link SecurableObject}
   */
  public static SecurableObject ofCatalog(String catalog) {
    checkName(catalog);

    return new SecurableObjectImpl(null, catalog, SecurableObject.Type.CATALOG);
  }

  /**
   * Create the schema {@link SecurableObject} with the given securable catalog object and schema
   * name.
   *
   * @param catalog The securable catalog object
   * @param schema The schema name
   * @return The created schema {@link SecurableObject}
   */
  public static SecurableObject ofSchema(SecurableObject catalog, String schema) {
    checkCatalog(catalog);
    checkName(schema);

    return new SecurableObjectImpl(catalog, schema, SecurableObject.Type.SCHEMA);
  }

  /**
   * Create the table {@link SecurableObject} with the given securable schema object and table name.
   *
   * @param schema The securable schema object
   * @param table The table name
   * @return The created table {@link SecurableObject}
   */
  public static SecurableObject ofTable(SecurableObject schema, String table) {
    checkSchema(schema);
    checkName(table);

    return new SecurableObjectImpl(schema, table, SecurableObject.Type.TABLE);
  }

  /**
   * Create the topic {@link SecurableObject} with the given securable schema object and topic name.
   *
   * @param schema The securable schema object
   * @param topic The topic name
   * @return The created topic {@link SecurableObject}
   */
  public static SecurableObject ofTopic(SecurableObject schema, String topic) {
    checkSchema(schema);
    checkName(topic);

    return new SecurableObjectImpl(schema, topic, SecurableObject.Type.TOPIC);
  }

  /**
   * Create the table {@link SecurableObject} with the given securable schema object and fileset
   * name.
   *
   * @param schema The securable schema object
   * @param fileset The fileset name
   * @return The created fileset {@link SecurableObject}
   */
  public static SecurableObject ofFileset(SecurableObject schema, String fileset) {
    checkSchema(schema);
    checkName(fileset);

    return new SecurableObjectImpl(schema, fileset, SecurableObject.Type.FILESET);
  }

  /**
   * All metalakes is a special securable object .You can give the securable object the privileges
   * `CREATE METALAKE`, etc. It means that you can create any which doesn't exist.
   *
   * @return The created {@link SecurableObject}
   */
  public static SecurableObject ofAllMetalakes() {
    return ALL_METALAKES;
  }

  private static void checkSchema(SecurableObject schema) {
    if (schema == null) {
      throw new IllegalArgumentException("Securable schema object can't be null");
    }
    checkCatalog(schema.parent());
  }

  private static void checkCatalog(SecurableObject catalog) {
    if (catalog == null) {
      throw new IllegalArgumentException("Securable catalog object can't be null");
    }

    if (catalog.parent() != null) {
      throw new IllegalArgumentException(
          String.format("The parent of securable catalog object %s must be null", catalog.name()));
    }
  }

  private static final SecurableObject ALL_METALAKES =
      new SecurableObjectImpl(null, "*", SecurableObject.Type.METALAKE);

  private static class SecurableObjectImpl implements SecurableObject {

    private final SecurableObject parent;
    private final String name;
    private final Type type;

    SecurableObjectImpl(SecurableObject parent, String name, Type type) {
      this.parent = parent;
      this.name = name;
      this.type = type;
    }

    @Override
    public SecurableObject parent() {
      return parent;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String fullName() {
      return toString();
    }

    @Override
    public Type type() {
      return type;
    }

    @Override
    public int hashCode() {
      return Objects.hash(parent, name, type);
    }

    @Override
    public String toString() {
      if (parent != null) {
        return parent.toString() + "." + name;
      } else {
        return name;
      }
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof SecurableObject)) {
        return false;
      }

      SecurableObject otherSecurableObject = (SecurableObject) other;
      return Objects.equals(parent, otherSecurableObject.parent())
          && Objects.equals(name, otherSecurableObject.name())
          && Objects.equals(type, otherSecurableObject.type());
    }
  }

  /**
   * Create a {@link SecurableObject} from the given identifier string.
   *
   * @param securableObjectIdentifier The identifier string
   * @param type The securable object type.
   * @return The created {@link SecurableObject}
   */
  public static SecurableObject parse(String securableObjectIdentifier, SecurableObject.Type type) {
    if ("*".equals(securableObjectIdentifier)) {
      if (type != SecurableObject.Type.METALAKE) {
        throw new IllegalArgumentException("If securable object isn't metalake, it can't be `*`");
      }
      return SecurableObjects.ofAllMetalakes();
    }

    if (StringUtils.isBlank(securableObjectIdentifier)) {
      throw new IllegalArgumentException("securable object identifier can't be blank");
    }

    Iterable<String> parts = DOT.split(securableObjectIdentifier);
    return SecurableObjects.of(type, Iterables.toArray(parts, String.class));
  }

  private static SecurableObject.Type getParentSecurableObjectType(SecurableObject.Type type) {
    switch (type) {
      case FILESET:
        return SecurableObject.Type.SCHEMA;

      case TOPIC:
        return SecurableObject.Type.SCHEMA;

      case TABLE:
        return SecurableObject.Type.SCHEMA;

      case SCHEMA:
        return SecurableObject.Type.CATALOG;

      case CATALOG:
        return SecurableObject.Type.METALAKE;

      default:
        throw new IllegalArgumentException(
            String.format("%s can't find its parent securable object type", type));
    }
  }

  private static void checkName(String name) {
    if (name == null) {
      throw new IllegalArgumentException("Cannot create a securable object with null name");
    }
  }
}
