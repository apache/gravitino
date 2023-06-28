package com.datastrato.graviton.catalog.hive.json;

import com.datastrato.graviton.*;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.Table;
import java.util.*;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang.StringUtils;

@EqualsAndHashCode
public class HiveTable implements Table, Entity, HasIdentifier, Auditable {
  public static final Field NAME = Field.required("name", String.class, "The name of the table");
  public static final Field COMMENT =
      Field.optional("comment", String.class, "The comment of the table");
  public static final Field PROPERTIES =
      Field.optional("properties", Map.class, "The properties of the table");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", AuditInfo.class, "The audit info of the table");
  private String name;

  private Namespace namespace;

  private String comment;

  private Map<String, String> properties;

  private AuditInfo auditInfo;

  private List<HiveColumn> columns;

  private org.apache.hadoop.hive.metastore.api.Table innerTable;

  // For Jackson Deserialization only.
  public HiveTable() {}

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = new HashMap<>();
    fields.put(NAME, name);
    fields.put(COMMENT, comment);
    fields.put(PROPERTIES, properties);
    fields.put(AUDIT_INFO, auditInfo);

    return Collections.unmodifiableMap(fields);
  }

  @Override
  public AuditInfo auditInfo() {
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(innerTable.getCreateTime() * 1000L);
    Date createTime = calendar.getTime();

    calendar.setTimeInMillis(innerTable.getLastAccessTime() * 1000L);
    Date lastAccessTime = calendar.getTime();

    return new AuditInfo.Builder()
        .withCreator(innerTable.getOwner())
        .withCreateTime(createTime.toInstant())
        .withLastModifiedTime(lastAccessTime.toInstant())
        .build();
  }

  @Override
  public String name() {
    if (StringUtils.isEmpty(name)) {
      name = innerTable.getTableName();
    }
    return name;
  }

  @Override
  public Namespace namespace() {
    if (namespace != null) {
      namespace = Namespace.of(innerTable.getDbName());
    }
    return namespace;
  }

  @Override
  public Column[] columns() {
    if (columns == null) {
      columns =
          innerTable.getSd().getCols().stream()
              .map(c -> new HiveColumn.Builder().withFieldSchema(c).build())
              .collect(Collectors.toList());
    }

    return columns.toArray(new Column[0]);
  }
}
