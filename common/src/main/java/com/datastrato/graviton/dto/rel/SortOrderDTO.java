/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.dto.rel;

import com.datastrato.graviton.dto.rel.ExpressionPartitionDTO.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import io.substrait.type.StringTypeVisitor;
import io.substrait.type.parser.ParseToPojo;
import io.substrait.type.parser.TypeStringParser;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@EqualsAndHashCode
@JsonPropertyOrder({"expression", "direction", "nullOrdering"})
@Getter
public class SortOrderDTO {
  public enum Direction {
    ASC,
    DESC;

    @JsonCreator
    public static Direction fromString(String value) {
      return Direction.valueOf(value.toUpperCase());
    }

    @JsonValue
    @Override
    public String toString() {
      return this.name().toLowerCase();
    }
  }

  public enum NullOrdering {
    FIRST,
    LAST;

    @JsonCreator
    public static NullOrdering fromString(String value) {
      return NullOrdering.valueOf(value.toUpperCase());
    }

    @JsonValue
    @Override
    public String toString() {
      return this.name().toLowerCase();
    }
  }

  @JsonProperty("expression")
  private final Expression expression;

  @JsonProperty("direction")
  private final Direction direction;

  @JsonProperty("nullOrdering")
  private final NullOrdering nullOrdering;

  /**
   * Creates a literal SortOrder instance, i.e. if we want to create a sort order on a literal like
   * sort/order by "'a' desc" where 'a' is the string literal
   *
   * <p>Then we can call this method like:
   *
   * <pre>
   * SortOrderDTO.literalSortOrder("a", "string", Direction.DESC, NullOrdering.FIRST)
   * </pre>
   *
   * @param literal value of the literal
   * @param type type of the literal, for example string, boolean, i32, i64, etc. For more
   *     information about the type, you can refer to {@link StringTypeVisitor}
   * @param direction direction of the sort order, i.e. ASC or DESC
   * @param nullOrdering null ordering of the sort order, i.e. FIRST or LAST
   * @return
   */
  public static SortOrderDTO literalSortOrder(
      String literal, String type, Direction direction, NullOrdering nullOrdering) {
    return new SortOrderDTO.Builder()
        .withDirection(direction)
        .withNullOrder(nullOrdering)
        .withExpression(
            new ExpressionPartitionDTO.LiteralExpression.Builder()
                .withType(TypeStringParser.parse(type, ParseToPojo::type))
                .withValue(literal)
                .build())
        .build();
  }

  /**
   * Creates a name reference sort order instance, i.e. if we want to create a sort order on a
   * literal like sort/order by "columnName desc" where 'columnName' is the column name
   *
   * <p>Then we can call this method like:
   *
   * <pre>
   * nameReferenceSortOrder(Direction.DESC,NullOrdering.FIRST, "columnName")
   * </pre>
   *
   * @param direction direction of the sort order, i.e. ASC or DESC
   * @param nullOrdering null ordering of the sort order, i.e. FIRST or LAST
   * @param nameReference name reference of the sort order, i.e. the name of the field
   * @return
   */
  public static SortOrderDTO nameReferenceSortOrder(
      Direction direction, NullOrdering nullOrdering, String... nameReference) {
    return new SortOrderDTO.Builder()
        .withDirection(direction)
        .withNullOrder(nullOrdering)
        .withExpression(
            new ExpressionPartitionDTO.FieldExpression.Builder()
                .withFieldName(nameReference)
                .build())
        .build();
  }

  @JsonCreator
  private SortOrderDTO(
      @JsonProperty("expression") Expression expression,
      @JsonProperty("direction") Direction direction,
      @JsonProperty("nullOrder") NullOrdering nullOrdering) {
    this.expression = expression;
    this.direction = direction;
    this.nullOrdering = nullOrdering;
  }

  public static class Builder {
    private Expression expression;
    private Direction direction;
    private NullOrdering nullOrdering;

    public Builder() {}

    public Builder withExpression(Expression expression) {
      this.expression = expression;
      return this;
    }

    public Builder withDirection(Direction direction) {
      this.direction = direction;
      return this;
    }

    public Builder withNullOrder(NullOrdering nullOrdering) {
      this.nullOrdering = nullOrdering;
      return this;
    }

    public SortOrderDTO build() {
      // Default direction is ASC
      this.direction = direction == null ? Direction.ASC : direction;

      // Default is nulls first
      this.nullOrdering = nullOrdering == null ? NullOrdering.FIRST : nullOrdering;

      Preconditions.checkNotNull(this.expression, "expression cannot be null");
      return new SortOrderDTO(expression, direction, nullOrdering);
    }
  }
}
