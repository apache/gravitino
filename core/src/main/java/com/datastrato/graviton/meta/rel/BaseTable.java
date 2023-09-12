/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.meta.rel;

import com.datastrato.graviton.Entity;
import com.datastrato.graviton.Field;
import com.datastrato.graviton.HasIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.Table;
import com.datastrato.graviton.rel.transforms.Transform;
import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.ToString;

/** An abstract class representing a base table in a relational database. */
@ToString
public class BaseTable implements Table, Entity, HasIdentifier {

  public static final Field ID = Field.required("id", Long.class, "The table's unique identifier");
  public static final Field NAME = Field.required("name", String.class, "The table's name");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment or description for the table");
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The associated properties of the table");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit details of the table");

  // we do not save columns in Graviton store, so it is an optional field
  public static final Field COLUMNS =
      Field.optional("columns", Column[].class, "The columns that make up the table");

  protected Long id;

  protected String name;

  @Nullable protected String comment;

  @Nullable protected Map<String, String> properties;

  protected AuditInfo auditInfo;

  protected Column[] columns;

  protected Namespace namespace;

  @Nullable protected Transform[] partitions;

  /**
   * Returns a map of the fields and their corresponding values for this table.
   *
   * @return A map of the fields and values.
   */
  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, id);
    fields.put(NAME, name);
    fields.put(COMMENT, comment);
    fields.put(PROPERTIES, properties);
    fields.put(AUDIT_INFO, auditInfo);
    fields.put(COLUMNS, columns);

    return fields;
  }

  /**
   * Returns the audit details of the table.
   *
   * @return The audit details of the table.
   */
  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  /**
   * Returns the name of the table.
   *
   * @return The name of the table.
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * Returns the unique id of the table.
   *
   * @return The unique id of the table.
   */
  @Override
  public Long id() {
    return id;
  }

  /**
   * Returns an array of columns that make up the table.
   *
   * @return An array of columns.
   */
  @Override
  public Column[] columns() {
    return columns;
  }

  /**
   * Returns the comment or description for the table.
   *
   * @return The comment or description for the table.
   */
  @Nullable
  @Override
  public String comment() {
    return comment;
  }

  /**
   * Returns the associated properties of the table.
   *
   * @return The associated properties of the table.
   */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * Returns the namespace of the table.
   *
   * @return The namespace of the table.
   */
  @Override
  public Namespace namespace() {
    return namespace;
  }

  @Override
  public Transform[] partitioning() {
    return partitions;
  }

  /**
   * Returns the type of the entity, which is {@link EntityType#TABLE}.
   *
   * @return The type of the entity.
   */
  @Override
  public EntityType type() {
    return EntityType.TABLE;
  }

  // Ignore field namespace, columns and comment
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BaseTable baseTable = (BaseTable) o;
    return Objects.equal(id, baseTable.id)
        && Objects.equal(name, baseTable.name)
        && Objects.equal(properties, baseTable.properties)
        && Objects.equal(auditInfo, baseTable.auditInfo);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id, name, comment, properties, auditInfo);
  }

  /**
   * Builder interface for creating instances of {@link BaseTable}.
   *
   * @param <SELF> The type of the builder.
   * @param <T> The type of the table being built.
   */
  interface Builder<SELF extends Builder<SELF, T>, T extends BaseTable> {
    SELF withId(Long id);

    SELF withNameSpace(Namespace namespace);

    SELF withName(String name);

    SELF withColumns(Column[] columns);

    SELF withComment(String comment);

    SELF withProperties(Map<String, String> properties);

    SELF withAuditInfo(AuditInfo auditInfo);

    SELF withPartitions(Transform[] partitions);

    T build();
  }

  /**
   * An abstract class implementing the builder interface for {@link BaseTable}.
   *
   * @param <SELF> The type of the builder.
   * @param <T> The type of the table being built.
   */
  public abstract static class BaseTableBuilder<SELF extends Builder<SELF, T>, T extends BaseTable>
      implements Builder<SELF, T> {
    protected Long id;
    protected String name;
    protected Namespace namespace;
    protected String comment;
    protected Map<String, String> properties;
    protected AuditInfo auditInfo;
    protected Column[] columns;
    protected Transform[] partitions;

    /**
     * Sets the unique identifier of the table.
     *
     * @param id The unique identifier of the table.
     * @return The builder instance.
     */
    @Override
    public SELF withId(Long id) {
      this.id = id;
      return self();
    }

    /**
     * Sets the namespace of the table.
     *
     * @param namespace The namespace of the table.
     * @return The builder instance.
     */
    @Override
    public SELF withNameSpace(Namespace namespace) {
      this.namespace = namespace;
      return self();
    }

    /**
     * Sets the name of the table.
     *
     * @param name The name of the table.
     * @return The builder instance.
     */
    @Override
    public SELF withName(String name) {
      this.name = name;
      return self();
    }

    /**
     * Sets the comment of the table.
     *
     * @param comment The comment or description for the table.
     * @return The builder instance.
     */
    @Override
    public SELF withComment(String comment) {
      this.comment = comment;
      return self();
    }

    /**
     * Sets the columns that make up the table.
     *
     * @param columns The columns that make up the table.
     * @return The builder instance.
     */
    @Override
    public SELF withColumns(Column[] columns) {
      this.columns = columns;
      return self();
    }

    /**
     * Sets the associated properties of the table.
     *
     * @param properties The associated properties of the table.
     * @return The builder instance.
     */
    @Override
    public SELF withProperties(Map<String, String> properties) {
      this.properties = properties;
      return self();
    }

    /**
     * Sets the audit details of the table.
     *
     * @param auditInfo The audit details of the table.
     * @return The builder instance.
     */
    @Override
    public SELF withAuditInfo(AuditInfo auditInfo) {
      this.auditInfo = auditInfo;
      return self();
    }

    @Override
    public SELF withPartitions(Transform[] partitions) {
      this.partitions = partitions;
      return self();
    }

    /**
     * Builds the instance of the table with the provided attributes.
     *
     * @return The built table instance.
     */
    @Override
    public T build() {
      T t = internalBuild();
      t.validate();
      return t;
    }

    private SELF self() {
      return (SELF) this;
    }

    protected abstract T internalBuild();
  }

  public static class TableBuilder extends BaseTableBuilder<TableBuilder, BaseTable> {
    @Override
    protected BaseTable internalBuild() {
      BaseTable table = new BaseTable();
      table.id = id;
      table.name = name;
      table.comment = comment;
      table.properties = properties;
      table.auditInfo = auditInfo;
      return table;
    }
  }
}
