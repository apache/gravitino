/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
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
  public static SecurableObject of(SecurableObjectType type, String... names) {

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

    List<SecurableObjectType> typeInDifferentLevels =
        Lists.newArrayList(SecurableObjectType.CATALOG, SecurableObjectType.SCHEMA);

    if (names.length == 1 && type != SecurableObjectType.CATALOG) {
      throw new IllegalArgumentException(
          "If the length of names is 1, it must be the CATALOG type");
    }

    if (names.length == 2 && type != SecurableObjectType.SCHEMA) {
      throw new IllegalArgumentException("If the length of names is 2, it must be the SCHEMA type");
    }

    if (names.length == 3
        && type != SecurableObjectType.FILESET
        && type != SecurableObjectType.TABLE
        && type != SecurableObjectType.TOPIC) {
      throw new IllegalArgumentException(
          "If the length of names is 3, it must be FILESET, TABLE or TOPIC");
    }

    if (names.length > 2) {
      typeInDifferentLevels.add(type);
    }

    SecurableObject parent = null;
    int level = 0;
    for (String name : names) {
      if (name == null) {
        throw new IllegalArgumentException("Cannot create a securable object with null name");
      }

      if (name.equals("*")) {
        throw new IllegalArgumentException(
            "Cannot create a securable object with `*` name. If you want to use a securable object which represents all catalogs,"
                + " you use the method `ofAllCatalogs`."
                + " If you want to create an another securable object which represents all entities,"
                + " you can use its parent entity, For example,"
                + " if you want to have read table privileges of all tables of `catalog1.schema1`,"
                + " you can use add `read table` privilege for `catalog1.schema1` directly");
      }

      parent = new SecurableObjectImpl(parent, name, typeInDifferentLevels.get(level));

      level++;
    }

    return parent;
  }

  /**
   * Create the catalog {@link SecurableObject} with the given catalog name.
   *
   * @param catalog The catalog name
   * @return The created catalog {@link SecurableObject}
   */
  public static SecurableObject ofCatalog(String catalog) {
    return of(SecurableObjectType.CATALOG, catalog);
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

    return of(SecurableObjectType.SCHEMA, catalog.name(), schema);
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

    return of(SecurableObjectType.TABLE, schema.parent().name(), schema.name(), table);
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

    return of(SecurableObjectType.TOPIC, schema.parent().name(), schema.name(), topic);
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

    return of(SecurableObjectType.FILESET, schema.parent().name(), schema.name(), fileset);
  }

  /**
   * All catalogs is a special securable object .You can give the securable object the privileges
   * `LOAD CATALOG`, `CREATE CATALOG`, etc. It means that you can load any catalog and create any
   * which doesn't exist.
   *
   * @return The created {@link SecurableObject}
   */
  public static SecurableObject ofAllCatalogs() {
    return ALL_CATALOGS;
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

  private static final SecurableObject ALL_CATALOGS =
      new SecurableObjectImpl(null, "*", SecurableObjectType.CATALOG);

  private static class SecurableObjectImpl implements SecurableObject {

    private final SecurableObject parent;
    private final String name;
    private final SecurableObjectType type;

    SecurableObjectImpl(SecurableObject parent, String name, SecurableObjectType type) {
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
    public SecurableObjectType type() {
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
  public static SecurableObject parse(String securableObjectIdentifier, SecurableObjectType type) {
    if ("*".equals(securableObjectIdentifier)) {
      return SecurableObjects.ofAllCatalogs();
    }

    if (StringUtils.isBlank(securableObjectIdentifier)) {
      throw new IllegalArgumentException("securable object identifier can't be blank");
    }

    Iterable<String> parts = DOT.split(securableObjectIdentifier);
    return SecurableObjects.of(type, Iterables.toArray(parts, String.class));
  }
}
