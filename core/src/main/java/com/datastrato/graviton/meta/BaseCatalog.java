package com.datastrato.graviton.meta;

import com.datastrato.graviton.*;
import java.io.Closeable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * This is the base class for a Catalog implementation, it defines the interface to initialize and
 * uninitialize the catalog. This class inherits from Closeable, so that a determined close() method
 * should be called to release any resources that the catalog holds.
 */
@EqualsAndHashCode
@ToString
public abstract class BaseCatalog implements Catalog, Entity, Auditable, HasIdentifier, Closeable {

  public static final Field ID =
      Field.required("id", Long.class, "The unique identifier of the catalog");
  public static final Field METALAKE_ID =
      Field.required("metalake_id", Long.class, "The unique identifier of the metalake");
  public static final Field NAME = Field.required("name", String.class, "The name of the catalog");
  public static final Field TYPE = Field.required("type", Type.class, "The type of the catalog");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment of the catalog");
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the catalog");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit info of the catalog");

  @Getter protected Long id;

  @Getter protected Long metalakeId;

  protected String name;

  protected Type type;

  @Nullable protected String comment;

  @Nullable protected Map<String, String> properties;

  protected AuditInfo auditInfo;

  protected Namespace namespace;

  /**
   * Initialize the catalog with specified configuration. This method is called after Catalog object
   * is created, but before any other method is called. The method is used to initialize the
   * connection to the underlying metadata source. RuntimeException will be thrown if the
   * initialization failed.
   *
   * @param config The configuration of this Catalog.
   * @throws RuntimeException if the initialization failed.
   */
  public abstract void initialize(Config config) throws RuntimeException;

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = new HashMap<>();
    fields.put(ID, id);
    fields.put(METALAKE_ID, metalakeId);
    fields.put(NAME, name);
    fields.put(COMMENT, comment);
    fields.put(TYPE, type);
    fields.put(PROPERTIES, properties);
    fields.put(AUDIT_INFO, auditInfo);

    return Collections.unmodifiableMap(fields);
  }

  @Override
  public Audit auditInfo() {
    return auditInfo;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Namespace namespace() {
    return namespace;
  }

  @Override
  public Type type() {
    return type;
  }

  @Override
  public String comment() {
    return comment;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  interface Builder<SELF extends Builder<SELF, T>, T extends BaseCatalog> {
    SELF withId(Long id);

    SELF withMetalakeId(Long metalakeId);

    SELF withName(String name);

    SELF withNamespace(Namespace namespace);

    SELF withType(Type type);

    SELF withComment(String comment);

    SELF withProperties(Map<String, String> properties);

    SELF withAuditInfo(AuditInfo auditInfo);

    T build();
  }

  public abstract static class BaseCatalogBuilder<
          SELF extends Builder<SELF, T>, T extends BaseCatalog>
      implements Builder<SELF, T> {
    protected Long id;
    protected Long metalakeId;
    protected String name;
    protected Namespace namespace;
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
    public SELF withMetalakeId(Long metalakeId) {
      this.metalakeId = metalakeId;
      return self();
    }

    @Override
    public SELF withName(String name) {
      this.name = name;
      return self();
    }

    @Override
    public SELF withNamespace(Namespace namespace) {
      this.namespace = namespace;
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
