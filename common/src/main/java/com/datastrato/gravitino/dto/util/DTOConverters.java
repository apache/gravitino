/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.util;

import static com.datastrato.gravitino.rel.expressions.transforms.Transforms.NAME_OF_IDENTITY;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Metalake;
import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.CatalogDTO;
import com.datastrato.gravitino.dto.MetalakeDTO;
import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.dto.rel.DistributionDTO;
import com.datastrato.gravitino.dto.rel.DistributionDTO.Builder;
import com.datastrato.gravitino.dto.rel.SchemaDTO;
import com.datastrato.gravitino.dto.rel.SortOrderDTO;
import com.datastrato.gravitino.dto.rel.TableDTO;
import com.datastrato.gravitino.dto.rel.expressions.FieldReferenceDTO;
import com.datastrato.gravitino.dto.rel.expressions.FuncExpressionDTO;
import com.datastrato.gravitino.dto.rel.expressions.FunctionArg;
import com.datastrato.gravitino.dto.rel.expressions.LiteralDTO;
import com.datastrato.gravitino.dto.rel.partitions.BucketPartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitions.DayPartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitions.FunctionPartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitions.HourPartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitions.IdentityPartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitions.ListPartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitions.MonthPartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitions.Partitioning;
import com.datastrato.gravitino.dto.rel.partitions.RangePartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitions.TruncatePartitioningDTO;
import com.datastrato.gravitino.dto.rel.partitions.YearPartitioningDTO;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Schema;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.expressions.FunctionExpression;
import com.datastrato.gravitino.rel.expressions.Literal;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrders;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.expressions.transforms.Transforms;
import java.util.Arrays;
import org.apache.commons.lang3.ArrayUtils;

public class DTOConverters {

  private DTOConverters() {}

  public static AuditDTO toDTO(Audit audit) {
    return AuditDTO.builder()
        .withCreator(audit.creator())
        .withCreateTime(audit.createTime())
        .withLastModifier(audit.lastModifier())
        .withLastModifiedTime(audit.lastModifiedTime())
        .build();
  }

  public static MetalakeDTO toDTO(Metalake metalake) {
    return new MetalakeDTO.Builder()
        .withName(metalake.name())
        .withComment(metalake.comment())
        .withProperties(metalake.properties())
        .withAudit(toDTO(metalake.auditInfo()))
        .build();
  }

  public static CatalogDTO toDTO(Catalog catalog) {
    return new CatalogDTO.Builder()
        .withName(catalog.name())
        .withType(catalog.type())
        .withProvider(catalog.provider())
        .withComment(catalog.comment())
        .withProperties(catalog.properties())
        .withAudit(toDTO(catalog.auditInfo()))
        .build();
  }

  public static SchemaDTO toDTO(Schema schema) {
    return new SchemaDTO.Builder()
        .withName(schema.name())
        .withComment(schema.comment())
        .withProperties(schema.properties())
        .withAudit(toDTO(schema.auditInfo()))
        .build();
  }

  public static ColumnDTO toDTO(Column column) {
    return new ColumnDTO.Builder()
        .withName(column.name())
        .withDataType(column.dataType())
        .withComment(column.comment())
        .withNullable(column.nullable())
        .build();
  }

  public static TableDTO toDTO(Table table) {
    return new TableDTO.Builder()
        .withName(table.name())
        .withComment(table.comment())
        .withColumns(
            Arrays.stream(table.columns()).map(DTOConverters::toDTO).toArray(ColumnDTO[]::new))
        .withProperties(table.properties())
        .withSortOrders(toDTOs(table.sortOrder()))
        .withDistribution(toDTO(table.distribution()))
        .withAudit(toDTO(table.auditInfo()))
        .withPartitioning(toDTOs(table.partitioning()))
        .build();
  }

  public static DistributionDTO toDTO(Distribution distribution) {
    if (Distributions.NONE.equals(distribution) || null == distribution) {
      return DistributionDTO.NONE;
    }

    if (distribution instanceof DistributionDTO) {
      return (DistributionDTO) distribution;
    }

    return new Builder()
        .withStrategy(distribution.strategy())
        .withNumber(distribution.number())
        .withArgs(
            Arrays.stream(distribution.expressions())
                .map(DTOConverters::toFunctionArg)
                .toArray(FunctionArg[]::new))
        .build();
  }

  public static SortOrderDTO toDTO(SortOrder sortOrder) {
    if (sortOrder instanceof SortOrderDTO) {
      return (SortOrderDTO) sortOrder;
    }
    return new SortOrderDTO.Builder()
        .withSortTerm(toFunctionArg(sortOrder.expression()))
        .withDirection(sortOrder.direction())
        .withNullOrder(sortOrder.nullOrdering())
        .build();
  }

  public static Partitioning toDTO(Transform transform) {
    if (transform instanceof Partitioning) {
      return (Partitioning) transform;
    }

    if (transform instanceof Transform.SingleFieldTransform) {
      String[] fieldName = ((Transform.SingleFieldTransform) transform).fieldName();
      switch (transform.name()) {
        case NAME_OF_IDENTITY:
          return IdentityPartitioningDTO.of(fieldName);
        case Transforms.NAME_OF_YEAR:
          return YearPartitioningDTO.of(fieldName);
        case Transforms.NAME_OF_MONTH:
          return MonthPartitioningDTO.of(fieldName);
        case Transforms.NAME_OF_DAY:
          return DayPartitioningDTO.of(fieldName);
        case Transforms.NAME_OF_HOUR:
          return HourPartitioningDTO.of(fieldName);
        default:
          throw new IllegalArgumentException("Unsupported transform: " + transform.name());
      }
    } else if (transform instanceof Transforms.BucketTransform) {
      return BucketPartitioningDTO.of(
          ((Transforms.BucketTransform) transform).numBuckets(),
          ((Transforms.BucketTransform) transform).fieldNames());
    } else if (transform instanceof Transforms.TruncateTransform) {
      return TruncatePartitioningDTO.of(
          ((Transforms.TruncateTransform) transform).width(),
          ((Transforms.TruncateTransform) transform).fieldName());
    } else if (transform instanceof Transforms.ListTransform) {
      return ListPartitioningDTO.of(((Transforms.ListTransform) transform).fieldNames());
    } else if (transform instanceof Transforms.RangeTransform) {
      return RangePartitioningDTO.of(((Transforms.RangeTransform) transform).fieldName());
    } else if (transform instanceof Transforms.ApplyTransform) {
      return FunctionPartitioningDTO.of(transform.name(), toFunctionArg(transform.arguments()));
    } else {
      throw new IllegalArgumentException("Unsupported transform: " + transform.name());
    }
  }

  public static FunctionArg toFunctionArg(Expression expression) {
    if (expression instanceof FunctionArg) {
      return (FunctionArg) expression;
    }

    if (expression instanceof Literal) {
      return new LiteralDTO.Builder()
          .withValue(((Literal<String>) expression).value())
          .withDataType(((Literal<String>) expression).dataType())
          .build();
    } else if (expression instanceof NamedReference.FieldReference) {
      return new FieldReferenceDTO.Builder()
          .withFieldName(((NamedReference.FieldReference) expression).fieldName())
          .build();
    } else if (expression instanceof FunctionExpression) {
      return new FuncExpressionDTO.Builder()
          .withFunctionName(((FunctionExpression) expression).functionName())
          .withFunctionArgs(
              Arrays.stream(((FunctionExpression) expression).arguments())
                  .map(DTOConverters::toFunctionArg)
                  .toArray(FunctionArg[]::new))
          .build();
    } else {
      throw new IllegalArgumentException("Unsupported expression type: " + expression.getClass());
    }
  }

  public static FunctionArg[] toFunctionArg(Expression[] expressions) {
    if (ArrayUtils.isEmpty(expressions)) {
      return FunctionArg.EMPTY_ARGS;
    }
    return Arrays.stream(expressions).map(DTOConverters::toFunctionArg).toArray(FunctionArg[]::new);
  }

  public static SortOrderDTO[] toDTOs(SortOrder[] sortOrders) {
    if (ArrayUtils.isEmpty(sortOrders)) {
      return new SortOrderDTO[0];
    }
    return Arrays.stream(sortOrders).map(DTOConverters::toDTO).toArray(SortOrderDTO[]::new);
  }

  public static Partitioning[] toDTOs(Transform[] transforms) {
    if (ArrayUtils.isEmpty(transforms)) {
      return new Partitioning[0];
    }
    return Arrays.stream(transforms).map(DTOConverters::toDTO).toArray(Partitioning[]::new);
  }

  public static Distribution fromDTO(DistributionDTO distributionDTO) {
    if (DistributionDTO.NONE.equals(distributionDTO) || null == distributionDTO) {
      return Distributions.NONE;
    }

    return Distributions.of(
        distributionDTO.strategy(),
        distributionDTO.number(),
        fromFunctionArg(distributionDTO.args()));
  }

  public static Expression[] fromFunctionArg(FunctionArg[] args) {
    if (ArrayUtils.isEmpty(args)) {
      return Expression.EMPTY_EXPRESSION;
    }
    return Arrays.stream(args).map(DTOConverters::fromFunctionArg).toArray(Expression[]::new);
  }

  public static Expression fromFunctionArg(FunctionArg arg) {
    switch (arg.argType()) {
      case LITERAL:
        return Literal.of(((LiteralDTO) arg).value(), ((LiteralDTO) arg).dataType());
      case FIELD:
        return NamedReference.field(((FieldReferenceDTO) arg).fieldName());
      case FUNCTION:
        return FunctionExpression.of(
            ((FuncExpressionDTO) arg).functionName(),
            fromFunctionArg(((FuncExpressionDTO) arg).args()));
      default:
        throw new IllegalArgumentException("Unsupported expression type: " + arg.getClass());
    }
  }

  public static SortOrder fromDTO(SortOrderDTO sortOrderDTO) {
    return SortOrders.of(
        fromFunctionArg(sortOrderDTO.sortTerm()),
        sortOrderDTO.direction(),
        sortOrderDTO.nullOrdering());
  }

  public static SortOrder[] fromDTOs(SortOrderDTO[] sortOrderDTO) {
    if (ArrayUtils.isEmpty(sortOrderDTO)) {
      return new SortOrder[0];
    }

    return Arrays.stream(sortOrderDTO).map(DTOConverters::fromDTO).toArray(SortOrder[]::new);
  }

  public static Transform[] fromDTOs(Partitioning[] partitioning) {
    if (ArrayUtils.isEmpty(partitioning)) {
      return new Transform[0];
    }
    return Arrays.stream(partitioning).map(DTOConverters::fromDTO).toArray(Transform[]::new);
  }

  public static Transform fromDTO(Partitioning partitioning) {
    switch (partitioning.strategy()) {
      case IDENTITY:
        return Transforms.identity(
            ((Partitioning.SingleFieldPartitioning) partitioning).fieldName());
      case YEAR:
        return Transforms.year(((Partitioning.SingleFieldPartitioning) partitioning).fieldName());
      case MONTH:
        return Transforms.month(((Partitioning.SingleFieldPartitioning) partitioning).fieldName());
      case DAY:
        return Transforms.day(((Partitioning.SingleFieldPartitioning) partitioning).fieldName());
      case HOUR:
        return Transforms.hour(((Partitioning.SingleFieldPartitioning) partitioning).fieldName());
      case BUCKET:
        return Transforms.bucket(
            ((BucketPartitioningDTO) partitioning).numBuckets(),
            ((BucketPartitioningDTO) partitioning).fieldNames());
      case TRUNCATE:
        return Transforms.truncate(
            ((TruncatePartitioningDTO) partitioning).width(),
            ((TruncatePartitioningDTO) partitioning).fieldName());
      case LIST:
        return Transforms.list(((ListPartitioningDTO) partitioning).fieldNames());
      case RANGE:
        return Transforms.range(((RangePartitioningDTO) partitioning).fieldName());
      case FUNCTION:
        return Transforms.apply(
            ((FunctionPartitioningDTO) partitioning).functionName(),
            fromFunctionArg(((FunctionPartitioningDTO) partitioning).args()));
      default:
        throw new IllegalArgumentException("Unsupported partitioning: " + partitioning.strategy());
    }
  }
}
