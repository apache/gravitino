/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel.partitioning;

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

/**
 * A partitioning strategy is a way to divide a table into smaller, more manageable pieces. This
 * interface represents a partitioning strategy.
 */
@JsonSerialize(using = JsonUtils.PartitioningSerializer.class)
@JsonDeserialize(using = JsonUtils.PartitioningDeserializer.class)
public interface Partitioning extends Transform {

  /** An empty array of partitioning. */
  Partitioning[] EMPTY_PARTITIONING = new Partitioning[0];

  /**
   * Returns the name of the partitioning strategy.
   *
   * @return The name of the partitioning strategy.
   */
  Strategy strategy();

  /**
   * Validates the partitioning columns.
   *
   * @param columns The columns to be validated.
   * @throws IllegalArgumentException If the columns are invalid, this exception is thrown.
   */
  void validate(ColumnDTO[] columns) throws IllegalArgumentException;

  /** Represents a partitioning strategy. */
  enum Strategy {
    /** The identity partitioning strategy. */
    IDENTITY,
    /** The year partitioning strategy. */
    YEAR,
    /** The month partitioning strategy. */
    MONTH,
    /** The day partitioning strategy. */
    DAY,
    /** The hour partitioning strategy. */
    HOUR,
    /** The minute partitioning strategy. */
    BUCKET,
    /** The truncate partitioning strategy. */
    TRUNCATE,
    /** The list partitioning strategy. */
    LIST,
    /** The range partitioning strategy. */
    RANGE,
    /** The hash partitioning strategy. */
    FUNCTION;

    /**
     * Get the partitioning strategy by name.
     *
     * @param name The name of the partitioning strategy.
     * @return The partitioning strategy.
     */
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

  /** A single field partitioning strategy. */
  @EqualsAndHashCode
  abstract class SingleFieldPartitioning implements Partitioning {
    String[] fieldName;

    /** @return The field name of the partitioning. */
    public String[] fieldName() {
      return fieldName;
    }

    /**
     * Validates the partitioning columns.
     *
     * @param columns The columns to be validated.
     * @throws IllegalArgumentException If the columns are invalid, this exception is thrown.
     */
    @Override
    public void validate(ColumnDTO[] columns) throws IllegalArgumentException {
      validateFieldExistence(columns, fieldName);
    }

    /** @return The name of the partitioning strategy. */
    @Override
    public String name() {
      return strategy().name().toLowerCase();
    }

    /** @return The arguments of the partitioning strategy. */
    @Override
    public Expression[] arguments() {
      return new Expression[] {field(fieldName)};
    }
  }
}
