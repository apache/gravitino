/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.requests;

import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rel.indexes.Indexes;
import com.datastrato.gravitino.rel.types.Type;
import com.datastrato.gravitino.rest.RESTRequest;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

/** Represents a request to update a table. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(value = TableUpdateRequest.RenameTableRequest.class, name = "rename"),
  @JsonSubTypes.Type(
      value = TableUpdateRequest.UpdateTableCommentRequest.class,
      name = "updateComment"),
  @JsonSubTypes.Type(
      value = TableUpdateRequest.SetTablePropertyRequest.class,
      name = "setProperty"),
  @JsonSubTypes.Type(
      value = TableUpdateRequest.RemoveTablePropertyRequest.class,
      name = "removeProperty"),
  @JsonSubTypes.Type(value = TableUpdateRequest.AddTableColumnRequest.class, name = "addColumn"),
  @JsonSubTypes.Type(
      value = TableUpdateRequest.RenameTableColumnRequest.class,
      name = "renameColumn"),
  @JsonSubTypes.Type(
      value = TableUpdateRequest.UpdateTableColumnTypeRequest.class,
      name = "updateColumnType"),
  @JsonSubTypes.Type(
      value = TableUpdateRequest.UpdateTableColumnCommentRequest.class,
      name = "updateColumnComment"),
  @JsonSubTypes.Type(
      value = TableUpdateRequest.UpdateTableColumnPositionRequest.class,
      name = "updateColumnPosition"),
  @JsonSubTypes.Type(
      value = TableUpdateRequest.UpdateTableColumnNullabilityRequest.class,
      name = "updateColumnNullability"),
  @JsonSubTypes.Type(
      value = TableUpdateRequest.DeleteTableColumnRequest.class,
      name = "deleteColumn"),
  @JsonSubTypes.Type(value = TableUpdateRequest.AddTableIndexRequest.class, name = "addTableIndex"),
  @JsonSubTypes.Type(
      value = TableUpdateRequest.DeleteTableIndexRequest.class,
      name = "deleteTableIndex"),
  @JsonSubTypes.Type(
      value = TableUpdateRequest.UpdateColumnAutoIncrementRequest.class,
      name = "updateColumnAutoIncrement")
})
public interface TableUpdateRequest extends RESTRequest {

  /**
   * The table change that is requested.
   *
   * @return An instance of TableChange.
   */
  TableChange tableChange();

  /** Represents a request to rename a table. */
  @EqualsAndHashCode
  @ToString
  class RenameTableRequest implements TableUpdateRequest {

    @Getter
    @JsonProperty("newName")
    private final String newName;

    /**
     * Constructor for RenameTableRequest.
     *
     * @param newName the new name of the table
     */
    public RenameTableRequest(String newName) {
      this.newName = newName;
    }

    /** Default constructor for Jackson deserialization. */
    public RenameTableRequest() {
      this(null);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(newName), "\"newName\" field is required and cannot be empty");
    }

    /**
     * Returns the table change.
     *
     * @return An instance of TableChange.
     */
    @Override
    public TableChange tableChange() {
      return TableChange.rename(newName);
    }
  }

  /** Represents a request to update the comment of a table. */
  @EqualsAndHashCode
  @ToString
  class UpdateTableCommentRequest implements TableUpdateRequest {

    @Getter
    @JsonProperty("newComment")
    private final String newComment;

    /**
     * Constructor for UpdateTableCommentRequest.
     *
     * @param newComment the new comment of the table
     */
    public UpdateTableCommentRequest(String newComment) {
      this.newComment = newComment;
    }

    /** Default constructor for Jackson deserialization. */
    public UpdateTableCommentRequest() {
      this(null);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(newComment),
          "\"newComment\" field is required and cannot be empty");
    }

    /**
     * Returns the table change.
     *
     * @return An instance of TableChange.
     */
    @Override
    public TableChange tableChange() {
      return TableChange.updateComment(newComment);
    }
  }

  /** Represents a request to set a property of a table. */
  @EqualsAndHashCode
  @ToString
  class SetTablePropertyRequest implements TableUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    @Getter
    @JsonProperty("value")
    private final String value;

    /**
     * Constructor for SetTablePropertyRequest.
     *
     * @param property the property to set
     * @param value the value to set
     */
    public SetTablePropertyRequest(String property, String value) {
      this.property = property;
      this.value = value;
    }

    /** Default constructor for Jackson deserialization. */
    public SetTablePropertyRequest() {
      this(null, null);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
      Preconditions.checkArgument(value != null, "\"value\" field is required and cannot be null");
    }

    /**
     * Returns the table change.
     *
     * @return An instance of TableChange.
     */
    @Override
    public TableChange tableChange() {
      return TableChange.setProperty(property, value);
    }
  }

  /** Represents a request to remove a property of a table. */
  @EqualsAndHashCode
  @ToString
  class RemoveTablePropertyRequest implements TableUpdateRequest {

    @Getter
    @JsonProperty("property")
    private final String property;

    /**
     * Constructor for RemoveTablePropertyRequest.
     *
     * @param property the property to remove
     */
    public RemoveTablePropertyRequest(String property) {
      this.property = property;
    }

    /** Default constructor for Jackson deserialization. */
    public RemoveTablePropertyRequest() {
      this(null);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(property), "\"property\" field is required and cannot be empty");
    }

    /** @return An instance of TableChange. */
    @Override
    public TableChange tableChange() {
      return TableChange.removeProperty(property);
    }
  }

  /** Represents a request to add a column to a table. */
  @EqualsAndHashCode
  @ToString
  class AddTableColumnRequest implements TableUpdateRequest {

    @Getter
    @JsonProperty("fieldName")
    private final String[] fieldName;

    @Getter
    @JsonProperty("type")
    @JsonSerialize(using = JsonUtils.TypeSerializer.class)
    @JsonDeserialize(using = JsonUtils.TypeDeserializer.class)
    private final Type dataType;

    @Getter
    @JsonProperty("comment")
    @Nullable
    private final String comment;

    @Getter
    @JsonProperty("position")
    @JsonSerialize(using = JsonUtils.ColumnPositionSerializer.class)
    @JsonDeserialize(using = JsonUtils.ColumnPositionDeserializer.class)
    @Nullable
    private final TableChange.ColumnPosition position;

    @Getter
    @JsonProperty(value = "nullable", defaultValue = "true")
    private final boolean nullable;

    @Getter
    @JsonProperty(value = "autoIncrement", defaultValue = "false")
    private final boolean autoIncrement;

    /** Default constructor for Jackson deserialization. */
    public AddTableColumnRequest() {
      this(null, null, null, null, true, false);
    }

    /**
     * Constructor for AddTableColumnRequest.
     *
     * @param fieldName the field name to add
     * @param dataType the data type of the field to add
     * @param comment the comment of the field to add
     * @param position the position of the field to add, null for default position
     * @param nullable whether the field to add is nullable
     * @param autoIncrement whether the field to add is auto increment
     */
    public AddTableColumnRequest(
        String[] fieldName,
        Type dataType,
        String comment,
        TableChange.ColumnPosition position,
        boolean nullable,
        boolean autoIncrement) {
      this.fieldName = fieldName;
      this.dataType = dataType;
      this.comment = comment;
      this.position = position == null ? TableChange.ColumnPosition.defaultPos() : position;
      this.nullable = nullable;
      this.autoIncrement = autoIncrement;
    }

    /**
     * Constructor for AddTableColumnRequest with default nullable value(true).
     *
     * @param fieldName the field name to add
     * @param dataType the data type of the field to add
     * @param comment the comment of the field to add
     * @param position the position of the field to add
     */
    public AddTableColumnRequest(
        String[] fieldName, Type dataType, String comment, TableChange.ColumnPosition position) {
      this(fieldName, dataType, comment, position, true, false);
    }

    /**
     * Constructor for AddTableColumnRequest with default position and nullable value(true).
     *
     * @param fieldName the field name to add
     * @param dataType the data type of the field to add
     * @param comment the comment of the field to add
     */
    public AddTableColumnRequest(String[] fieldName, Type dataType, String comment) {
      this(fieldName, dataType, comment, TableChange.ColumnPosition.defaultPos());
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          fieldName != null
              && fieldName.length > 0
              && Arrays.stream(fieldName).allMatch(StringUtils::isNotBlank),
          "\"fieldName\" field is required and cannot be empty");
      Preconditions.checkArgument(
          dataType != null, "\"type\" field is required and cannot be empty");
    }

    /** @return An instance of TableChange. */
    @Override
    public TableChange tableChange() {
      return TableChange.addColumn(fieldName, dataType, comment, position, nullable, autoIncrement);
    }
  }

  /** Represents a request to rename a column of a table. */
  @EqualsAndHashCode
  @ToString
  class RenameTableColumnRequest implements TableUpdateRequest {

    @Getter
    @JsonProperty("oldFieldName")
    private final String[] oldFieldName;

    @Getter
    @JsonProperty("newFieldName")
    private final String newFieldName;

    /**
     * Constructor for RenameTableColumnRequest.
     *
     * @param oldFieldName the old field name to rename
     * @param newFieldName the new field name
     */
    public RenameTableColumnRequest(String[] oldFieldName, String newFieldName) {
      this.oldFieldName = oldFieldName;
      this.newFieldName = newFieldName;
    }

    /** Default constructor for Jackson deserialization. */
    public RenameTableColumnRequest() {
      this(null, null);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          oldFieldName != null
              && oldFieldName.length > 0
              && Arrays.stream(oldFieldName).allMatch(StringUtils::isNotBlank),
          "\"oldFieldName\" field is required and cannot be empty");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(newFieldName),
          "\"newFieldName\" field is required and cannot be empty");
    }

    /** @return An instance of TableChange. */
    @Override
    public TableChange tableChange() {
      return TableChange.renameColumn(oldFieldName, newFieldName);
    }
  }

  /** Represents a request to update the type of a column of a table. */
  @EqualsAndHashCode
  @ToString
  class UpdateTableColumnTypeRequest implements TableUpdateRequest {

    @Getter
    @JsonProperty("fieldName")
    private final String[] fieldName;

    @Getter
    @JsonProperty("newType")
    @JsonSerialize(using = JsonUtils.TypeSerializer.class)
    @JsonDeserialize(using = JsonUtils.TypeDeserializer.class)
    private final Type newType;

    /**
     * Constructor for UpdateTableColumnTypeRequest.
     *
     * @param fieldName the field name to update
     * @param newType the new type of the field
     */
    public UpdateTableColumnTypeRequest(String[] fieldName, Type newType) {
      this.fieldName = fieldName;
      this.newType = newType;
    }

    /** Default constructor for Jackson deserialization. */
    public UpdateTableColumnTypeRequest() {
      this(null, null);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          fieldName != null
              && fieldName.length > 0
              && Arrays.stream(fieldName).allMatch(StringUtils::isNotBlank),
          "\"fieldName\" field is required and cannot be empty");
      Preconditions.checkArgument(
          newType != null, "\"newType\" field is required and cannot be empty");
    }

    /** @return An instance of TableChange. */
    @Override
    public TableChange tableChange() {
      return TableChange.updateColumnType(fieldName, newType);
    }
  }

  /** Represents a request to update the comment of a column of a table. */
  @EqualsAndHashCode
  @ToString
  class UpdateTableColumnCommentRequest implements TableUpdateRequest {

    @Getter
    @JsonProperty("fieldName")
    private final String[] fieldName;

    @Getter
    @JsonProperty("newComment")
    private final String newComment;

    /**
     * Constructor for UpdateTableColumnCommentRequest.
     *
     * @param fieldName the field name to update
     * @param newComment the new comment of the field
     */
    public UpdateTableColumnCommentRequest(String[] fieldName, String newComment) {
      this.fieldName = fieldName;
      this.newComment = newComment;
    }

    /** Default constructor for Jackson deserialization. */
    public UpdateTableColumnCommentRequest() {
      this(null, null);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          fieldName != null
              && fieldName.length > 0
              && Arrays.stream(fieldName).allMatch(StringUtils::isNotBlank),
          "\"fieldName\" field is required and cannot be empty");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(newComment),
          "\"newComment\" field is required and cannot be empty");
    }

    /** @return An instance of TableChange. */
    @Override
    public TableChange tableChange() {
      return TableChange.updateColumnComment(fieldName, newComment);
    }
  }

  /** Represents a request to update the position of a column of a table. */
  @EqualsAndHashCode
  @ToString
  class UpdateTableColumnPositionRequest implements TableUpdateRequest {

    @Getter
    @JsonProperty("fieldName")
    private final String[] fieldName;

    @Getter
    @JsonProperty("newPosition")
    @JsonSerialize(using = JsonUtils.ColumnPositionSerializer.class)
    @JsonDeserialize(using = JsonUtils.ColumnPositionDeserializer.class)
    private final TableChange.ColumnPosition newPosition;

    /**
     * Constructor for UpdateTableColumnPositionRequest.
     *
     * @param fieldName the field name to update
     * @param newPosition the new position of the field
     */
    public UpdateTableColumnPositionRequest(
        String[] fieldName, TableChange.ColumnPosition newPosition) {
      this.fieldName = fieldName;
      this.newPosition = newPosition;
    }

    /** Default constructor for Jackson deserialization. */
    public UpdateTableColumnPositionRequest() {
      this(null, null);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          fieldName != null
              && fieldName.length > 0
              && Arrays.stream(fieldName).allMatch(StringUtils::isNotBlank),
          "\"fieldName\" field is required and cannot be empty");
      Preconditions.checkArgument(
          newPosition != null, "\"newPosition\" field is required and cannot be empty");
    }

    /** @return An instance of TableChange. */
    @Override
    public TableChange tableChange() {
      return TableChange.updateColumnPosition(fieldName, newPosition);
    }
  }

  /** Represents a request to update the nullability of a column of a table. */
  @EqualsAndHashCode
  @ToString
  class UpdateTableColumnNullabilityRequest implements TableUpdateRequest {

    @Getter
    @JsonProperty("fieldName")
    private final String[] fieldName;

    @Getter
    @JsonProperty("nullable")
    private final boolean nullable;

    /**
     * Constructor for UpdateTableColumnNullabilityRequest.
     *
     * @param fieldName the field name to update
     * @param nullable the new nullability of the field
     */
    public UpdateTableColumnNullabilityRequest(String[] fieldName, boolean nullable) {
      this.fieldName = fieldName;
      this.nullable = nullable;
    }

    /** Default constructor for Jackson deserialization. */
    public UpdateTableColumnNullabilityRequest() {
      this(null, true);
    }

    /**
     * Validates the request.
     *
     * @return An instance of TableChange.
     */
    @Override
    public TableChange tableChange() {
      return TableChange.updateColumnNullability(fieldName, nullable);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          fieldName != null
              && fieldName.length > 0
              && Arrays.stream(fieldName).allMatch(StringUtils::isNotBlank),
          "\"fieldName\" field is required and cannot be empty");
    }
  }

  /** Represents a request to delete a column from a table. */
  @EqualsAndHashCode
  @ToString
  class DeleteTableColumnRequest implements TableUpdateRequest {

    @Getter
    @JsonProperty("fieldName")
    private final String[] fieldName;

    @Getter
    @JsonProperty("ifExists")
    private final boolean ifExists;

    /**
     * Constructor for DeleteTableColumnRequest.
     *
     * @param fieldName the field name to delete
     * @param ifExists whether to delete the column if it exists
     */
    public DeleteTableColumnRequest(String[] fieldName, boolean ifExists) {
      this.fieldName = fieldName;
      this.ifExists = ifExists;
    }

    /** Default constructor for Jackson deserialization. */
    public DeleteTableColumnRequest() {
      this(null, false);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          fieldName != null
              && fieldName.length > 0
              && Arrays.stream(fieldName).allMatch(StringUtils::isNotBlank),
          "\"fieldName\" field is required and cannot be empty");
    }

    /** @return An instance of TableChange. */
    @Override
    public TableChange tableChange() {
      return TableChange.deleteColumn(fieldName, ifExists);
    }
  }

  /** Represents a request to add an index to a table. */
  @EqualsAndHashCode
  @ToString
  class AddTableIndexRequest implements TableUpdateRequest {

    @JsonProperty("index")
    @JsonSerialize(using = JsonUtils.IndexSerializer.class)
    @JsonDeserialize(using = JsonUtils.IndexDeserializer.class)
    private Index index;

    /** Default constructor for Jackson deserialization. */
    public AddTableIndexRequest() {}

    /**
     * The constructor of the add table index request.
     *
     * @param type The type of the index
     * @param name The name of the index
     * @param fieldNames The field names under the table contained in the index.
     */
    public AddTableIndexRequest(Index.IndexType type, String name, String[][] fieldNames) {
      this.index = Indexes.of(type, name, fieldNames);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkNotNull(index, "Index cannot be null");
      Preconditions.checkArgument(index.type() != null, "Index type cannot be null");
      Preconditions.checkArgument(
          index.fieldNames() != null && index.fieldNames().length > 0,
          "The index must be set with corresponding column names");
    }

    /** @return An instance of TableChange. */
    @Override
    public TableChange tableChange() {
      return TableChange.addIndex(index.type(), index.name(), index.fieldNames());
    }
  }

  /** Represents a request to delete an index from a table. */
  @EqualsAndHashCode
  @ToString
  class DeleteTableIndexRequest implements TableUpdateRequest {

    @JsonProperty("name")
    private String name;

    @JsonProperty("ifExists")
    private Boolean ifExists;

    /** Default constructor for Jackson deserialization. */
    public DeleteTableIndexRequest() {}

    /**
     * The constructor of the delete table index request.
     *
     * @param name The name of the index
     * @param ifExists Whether to delete the index if it exists
     */
    public DeleteTableIndexRequest(String name, Boolean ifExists) {
      this.name = name;
      this.ifExists = ifExists;
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkNotNull(name, "Index name cannot be null");
    }

    /** @return An instance of TableChange. */
    @Override
    public TableChange tableChange() {
      return TableChange.deleteIndex(name, ifExists);
    }
  }

  /** Represents a request to update a column autoIncrement from a table. */
  @EqualsAndHashCode
  @ToString
  class UpdateColumnAutoIncrementRequest implements TableUpdateRequest {

    @Getter
    @JsonProperty("fieldName")
    private final String[] fieldName;

    @Getter
    @JsonProperty("autoIncrement")
    private final boolean autoIncrement;

    /**
     * Constructor for UpdateColumnAutoIncrementRequest.
     *
     * @param fieldName the field name to update.
     * @param autoIncrement Whether the column is auto-incremented.
     */
    public UpdateColumnAutoIncrementRequest(String[] fieldName, boolean autoIncrement) {
      this.fieldName = fieldName;
      this.autoIncrement = autoIncrement;
    }

    /** Default constructor for Jackson deserialization. */
    public UpdateColumnAutoIncrementRequest() {
      this(null, false);
    }

    /**
     * Validates the request.
     *
     * @throws IllegalArgumentException If the request is invalid, this exception is thrown.
     */
    @Override
    public void validate() throws IllegalArgumentException {
      Preconditions.checkArgument(
          fieldName != null
              && fieldName.length > 0
              && Arrays.stream(fieldName).allMatch(StringUtils::isNotBlank),
          "\"fieldName\" field is required and cannot be empty");
    }

    /** @return An instance of TableChange. */
    @Override
    public TableChange tableChange() {
      return TableChange.updateColumnAutoIncrement(fieldName, autoIncrement);
    }
  }
}
