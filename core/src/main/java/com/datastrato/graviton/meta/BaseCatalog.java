package com.datastrato.graviton.meta;

import com.datastrato.graviton.*;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Closeable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * This is the base interface for a Catalog implementation, it defines the interface to
 * initialize and uninitialize the catalog. This interface inherits from Closeable, so that a
 * determined close() method should be called to release any resources that the catalog holds.
 */
@Getter
@EqualsAndHashCode
@ToString
public abstract class BaseCatalog implements Entity, Auditable, HasIdentifier, Closeable {

  enum Type {
    RELATIONAL, // Catalog Type for Relational Data Structure, like db.table, catalog.db.table.
    FILE, // Catalog Type for File System (including HDFS, S3, etc.), like path/to/file
    STREAM, // Catalog Type for Streaming Data, like kafka://topic
  }

  public static final Field ID =
      Field.required("id", Long.class, "The unique identifier of the catalog");
  public static final Field LAKEHOUSE_ID =
      Field.required("lakehouse_id", Long.class, "The unique identifier of the lakehouse");
  public static final Field NAME = Field.required("name", String.class, "The name of the catalog");
  public static final Field TYPE = Field.required("type", Type.class, "The type of the catalog");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment of the catalog");
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the catalog");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit info of the catalog");

  @JsonProperty("id")
  private Long id;

  @JsonProperty("lakehouse_id")
  private Long lakehouseId;

  @JsonProperty("name")
  private String name;

  @JsonProperty("type")
  private Type type;

  @Nullable
  @JsonProperty("comment")
  private String comment;

  @Nullable
  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("audit_info")
  private AuditInfo auditInfo;

  /**
   * Initialize the catalog with specified configuration. This method is called after
   * Catalog object is created, but before any other method is called. The method is used to
   * initialize the connection to the underlying metadata source. RuntimeException will be thrown
   * if the initialization failed.
   *
   * @param config The configuration of this Catalog.
   * @throws RuntimeException if the initialization failed.
   */
  abstract void initialize(Config config) throws RuntimeException;

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = new HashMap<>();
    fields.put(ID, id);
    fields.put(LAKEHOUSE_ID, lakehouseId);
    fields.put(NAME, name);
    fields.put(COMMENT, comment);
    fields.put(TYPE, type);
    fields.put(PROPERTIES, properties);
    fields.put(AUDIT_INFO, auditInfo);

    return Collections.unmodifiableMap(fields);
  }

  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  @Override
  public String name() {
    return name;
  }

  interface Builder<SELF extends Builder<SELF, T>, T extends BaseCatalog> {
    SELF withId(Long id);

    SELF withLakehouseId(Long lakehouseId);

    SELF withName(String name);

    SELF withType(Type type);

    SELF withComment(String comment);

    SELF withProperties(Map<String, String> properties);

    SELF withAuditInfo(AuditInfo auditInfo);

    T build();
  }

  public static abstract class BaseCatalogBuilder<
      SELF extends Builder<SELF, T>, T extends BaseCatalog> implements Builder<SELF, T> {
    protected Long id;
    protected Long lakehouseId;
    protected String name;
    protected Type type;
    protected String comment;
    protected Map<String, String> properties;
    protected AuditInfo auditInfo;

    @Override
    public SELF withId(Long id) {
      this.id = id;
      return self();
    }

    @Override
    public SELF withLakehouseId(Long lakehouseId) {
      this.lakehouseId = lakehouseId;
      return self();
    }

    @Override
    public SELF withName(String name) {
      this.name = name;
      return self();
    }

    @Override
    public SELF withType(Type type) {
      this.type = type;
      return self();
    }

    @Override
    public SELF withComment(String comment) {
      this.comment = comment;
      return self();
    }

    @Override
    public SELF withProperties(Map<String, String> properties) {
      this.properties = properties;
      return self();
    }

    @Override
    public SELF withAuditInfo(AuditInfo auditInfo) {
      this.auditInfo = auditInfo;
      return self();
    }

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
}
