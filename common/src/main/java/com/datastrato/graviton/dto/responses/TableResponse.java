/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.dto.responses;

import com.datastrato.graviton.dto.rel.TableDTO;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class TableResponse extends BaseResponse {

  @JsonProperty("table")
  private final TableDTO table;

  public TableResponse(TableDTO table) {
    super(0);
    this.table = table;
  }

  // This is the constructor that is used by Jackson deserializer
  public TableResponse() {
    super();
    this.table = null;
  }

  @Override
  public void validate() throws IllegalArgumentException {
    super.validate();

    Preconditions.checkArgument(table != null, "table must not be null");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(table.name()), "table 'name' must not be null and empty");
    Preconditions.checkArgument(
        table.columns() != null && table.columns().length > 0,
        "table 'columns' must not be null and empty");
    Preconditions.checkArgument(table.auditInfo() != null, "table 'audit' must not be null");
  }
}
