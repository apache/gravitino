/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.dto.rel.partitioning;

import static org.apache.gravitino.rel.expressions.NamedReference.field;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Arrays;
import lombok.EqualsAndHashCode;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.PartitionUtils;
import org.apache.gravitino.json.JsonUtils.PartitioningDeserializer;
import org.apache.gravitino.json.JsonUtils.PartitioningSerializer;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.transforms.Transform;

/**
 * A partitioning strategy is a way to divide a table into smaller, more manageable pieces. This
 * interface represents a partitioning strategy.
 */
@JsonSerialize(using = PartitioningSerializer.class)
@JsonDeserialize(using = PartitioningDeserializer.class)
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

    /**
     * @return The field name of the partitioning.
     */
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
      PartitionUtils.validateFieldExistence(columns, fieldName);
    }

    /**
     * @return The name of the partitioning strategy.
     */
    @Override
    public String name() {
      return strategy().name().toLowerCase();
    }

    /**
     * @return The arguments of the partitioning strategy.
     */
    @Override
    public Expression[] arguments() {
      return new Expression[] {field(fieldName)};
    }
  }
}
