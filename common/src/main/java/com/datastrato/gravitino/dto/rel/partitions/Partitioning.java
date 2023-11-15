/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.partitions;

import static com.datastrato.gravitino.dto.rel.PartitionUtils.validateFieldExistence;
import static com.datastrato.gravitino.rel.expressions.NamedReference.field;

import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.json.JsonUtils;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Arrays;
import lombok.EqualsAndHashCode;

@JsonSerialize(using = JsonUtils.PartitioningSerializer.class)
@JsonDeserialize(using = JsonUtils.PartitioningDeserializer.class)
public interface Partitioning extends Transform {

  Partitioning[] EMPTY_PARTITIONING = new Partitioning[0];

  Strategy strategy();

  // columns of table
  void validate(ColumnDTO[] columns) throws IllegalArgumentException;

  enum Strategy {
    IDENTITY,
    YEAR,
    MONTH,
    DAY,
    HOUR,
    BUCKET,
    TRUNCATE,
    LIST,
    RANGE,
    FUNCTION;

    public static Strategy getByName(String name) {
      for (Strategy strategy : Strategy.values()) {
        if (strategy.name().equalsIgnoreCase(name)) {
          return strategy;
        }
      }
      throw new IllegalArgumentException(
          "Invalid partitioning strategy: "
              + name
              + ". Valid values are: "
              + Arrays.toString(Strategy.values()));
    }
  }

  @EqualsAndHashCode
  abstract class SingleFieldPartitioning implements Partitioning {
    String[] fieldName;

    public String[] fieldName() {
      return fieldName;
    }

    @Override
    public void validate(ColumnDTO[] columns) throws IllegalArgumentException {
      validateFieldExistence(columns, fieldName);
    }

    @Override
    public String name() {
      return strategy().name().toLowerCase();
    }

    @Override
    public Expression[] arguments() {
      return new Expression[] {field(fieldName)};
    }
  }
}
