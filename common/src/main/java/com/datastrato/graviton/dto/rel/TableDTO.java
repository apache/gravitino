/*·Copyright·2023·Datastrato.·This·software·is·licensed·under·the·Apache·License·version·2.·*/
package com.datastrato.graviton.dto.rel;

import com.datastrato.graviton.dto.AuditDTO;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.Table;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;

public class TableDTO implements Table {

  @JsonProperty("name")
  private String name;

  @JsonProperty("comment")
  private String comment;

  @JsonProperty("columns")
  private ColumnDTO[] columns;

  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("audit")
  private AuditDTO audit;

  private TableDTO() {}

  private TableDTO(
      String name,
      String comment,
      ColumnDTO[] columns,
      Map<String, String> properties,
      AuditDTO audit) {
    this.name = name;
    this.comment = comment;
    this.columns = columns;
    this.properties = properties;
    this.audit = audit;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Column[] columns() {
    return columns;
  }

  @Override
  public String comment() {
    return comment;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public AuditDTO auditInfo() {
    return audit;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder<S extends Builder> {
    protected String name;
    protected String comment;
    protected ColumnDTO[] columns;
    protected Map<String, String> properties;
    protected AuditDTO audit;

    public Builder() {}

    public S withName(String name) {
      this.name = name;
      return (S) this;
    }

    public S withComment(String comment) {
      this.comment = comment;
      return (S) this;
    }

    public S withColumns(ColumnDTO[] columns) {
      this.columns = columns;
      return (S) this;
    }

    public S withProperties(Map<String, String> properties) {
      this.properties = properties;
      return (S) this;
    }

    public S withAudit(AuditDTO audit) {
      this.audit = audit;
      return (S) this;
    }

    public TableDTO build() {
      Preconditions.checkArgument(name != null && !name.isEmpty(), "name cannot be null or empty");
      Preconditions.checkArgument(
          columns != null && columns.length > 0, "columns cannot be null or empty");
      Preconditions.checkArgument(audit != null, "audit cannot be null");

      return new TableDTO(name, comment, columns, properties, audit);
    }
  }
}
