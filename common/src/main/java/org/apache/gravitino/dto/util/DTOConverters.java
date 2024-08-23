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
package org.apache.gravitino.dto.util;

import static org.apache.gravitino.rel.expressions.transforms.Transforms.NAME_OF_IDENTITY;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.Schema;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.CatalogDTO;
import org.apache.gravitino.dto.MetalakeDTO;
import org.apache.gravitino.dto.SchemaDTO;
import org.apache.gravitino.dto.authorization.GroupDTO;
import org.apache.gravitino.dto.authorization.OwnerDTO;
import org.apache.gravitino.dto.authorization.PrivilegeDTO;
import org.apache.gravitino.dto.authorization.RoleDTO;
import org.apache.gravitino.dto.authorization.SecurableObjectDTO;
import org.apache.gravitino.dto.authorization.UserDTO;
import org.apache.gravitino.dto.file.FilesetContextDTO;
import org.apache.gravitino.dto.file.FilesetDTO;
import org.apache.gravitino.dto.messaging.TopicDTO;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.DistributionDTO;
import org.apache.gravitino.dto.rel.SortOrderDTO;
import org.apache.gravitino.dto.rel.TableDTO;
import org.apache.gravitino.dto.rel.expressions.FieldReferenceDTO;
import org.apache.gravitino.dto.rel.expressions.FuncExpressionDTO;
import org.apache.gravitino.dto.rel.expressions.FunctionArg;
import org.apache.gravitino.dto.rel.expressions.LiteralDTO;
import org.apache.gravitino.dto.rel.expressions.UnparsedExpressionDTO;
import org.apache.gravitino.dto.rel.indexes.IndexDTO;
import org.apache.gravitino.dto.rel.partitioning.BucketPartitioningDTO;
import org.apache.gravitino.dto.rel.partitioning.DayPartitioningDTO;
import org.apache.gravitino.dto.rel.partitioning.FunctionPartitioningDTO;
import org.apache.gravitino.dto.rel.partitioning.HourPartitioningDTO;
import org.apache.gravitino.dto.rel.partitioning.IdentityPartitioningDTO;
import org.apache.gravitino.dto.rel.partitioning.ListPartitioningDTO;
import org.apache.gravitino.dto.rel.partitioning.MonthPartitioningDTO;
import org.apache.gravitino.dto.rel.partitioning.Partitioning;
import org.apache.gravitino.dto.rel.partitioning.RangePartitioningDTO;
import org.apache.gravitino.dto.rel.partitioning.TruncatePartitioningDTO;
import org.apache.gravitino.dto.rel.partitioning.YearPartitioningDTO;
import org.apache.gravitino.dto.rel.partitions.IdentityPartitionDTO;
import org.apache.gravitino.dto.rel.partitions.ListPartitionDTO;
import org.apache.gravitino.dto.rel.partitions.PartitionDTO;
import org.apache.gravitino.dto.rel.partitions.RangePartitionDTO;
import org.apache.gravitino.dto.tag.MetadataObjectDTO;
import org.apache.gravitino.dto.tag.TagDTO;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetContext;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.UnparsedExpression;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.partitions.IdentityPartition;
import org.apache.gravitino.rel.partitions.ListPartition;
import org.apache.gravitino.rel.partitions.Partition;
import org.apache.gravitino.rel.partitions.Partitions;
import org.apache.gravitino.rel.partitions.RangePartition;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.tag.Tag;

/** Utility class for converting between DTOs and domain objects. */
public class DTOConverters {

  private DTOConverters() {}

  /**
   * Converts a {@link Audit} to a {@link AuditDTO}.
   *
   * @param audit The audit.
   * @return The audit DTO.
   */
  public static AuditDTO toDTO(Audit audit) {
    return AuditDTO.builder()
        .withCreator(audit.creator())
        .withCreateTime(audit.createTime())
        .withLastModifier(audit.lastModifier())
        .withLastModifiedTime(audit.lastModifiedTime())
        .build();
  }

  /**
   * Converts a {@link Owner} to a {@link OwnerDTO}.
   *
   * @param owner The owner.
   * @return The owner DTO.
   */
  public static OwnerDTO toDTO(Owner owner) {
    return OwnerDTO.builder().withName(owner.name()).withType(owner.type()).build();
  }

  /**
   * Converts a {@link Metalake} to a {@link MetalakeDTO}.
   *
   * @param metalake The metalake.
   * @return The metalake DTO.
   */
  public static MetalakeDTO toDTO(Metalake metalake) {
    return MetalakeDTO.builder()
        .withName(metalake.name())
        .withComment(metalake.comment())
        .withProperties(metalake.properties())
        .withAudit(toDTO(metalake.auditInfo()))
        .build();
  }

  /**
   * Converts a {@link Partition} to a {@link PartitionDTO}.
   *
   * @param partition The partition.
   * @return The partition DTO.
   */
  public static PartitionDTO toDTO(Partition partition) {
    if (partition instanceof RangePartition) {
      RangePartition rangePartition = (RangePartition) partition;
      return RangePartitionDTO.builder()
          .withName(rangePartition.name())
          .withUpper((LiteralDTO) toFunctionArg(rangePartition.upper()))
          .withLower((LiteralDTO) toFunctionArg(rangePartition.lower()))
          .withProperties(rangePartition.properties())
          .build();
    } else if (partition instanceof IdentityPartition) {
      IdentityPartition identityPartition = (IdentityPartition) partition;
      return IdentityPartitionDTO.builder()
          .withName(identityPartition.name())
          .withFieldNames(identityPartition.fieldNames())
          .withValues(
              Arrays.stream(identityPartition.values())
                  .map(v -> (LiteralDTO) toFunctionArg(v))
                  .toArray(LiteralDTO[]::new))
          .withProperties(identityPartition.properties())
          .build();
    } else if (partition instanceof ListPartition) {
      ListPartition listPartition = (ListPartition) partition;
      return ListPartitionDTO.builder()
          .withName(listPartition.name())
          .withLists(
              Arrays.stream(listPartition.lists())
                  .map(
                      list ->
                          Arrays.stream(list)
                              .map(v -> (LiteralDTO) toFunctionArg(v))
                              .toArray(LiteralDTO[]::new))
                  .toArray(LiteralDTO[][]::new))
          .withProperties(listPartition.properties())
          .build();
    } else {
      throw new IllegalArgumentException("Unsupported partition type: " + partition.getClass());
    }
  }

  /**
   * Converts a {@link Catalog} to a {@link CatalogDTO}.
   *
   * @param catalog The catalog.
   * @return The catalog DTO.
   */
  public static CatalogDTO toDTO(Catalog catalog) {
    return CatalogDTO.builder()
        .withName(catalog.name())
        .withType(catalog.type())
        .withProvider(catalog.provider())
        .withComment(catalog.comment())
        .withProperties(catalog.properties())
        .withAudit(toDTO(catalog.auditInfo()))
        .build();
  }

  /**
   * Converts a {@link Schema} to a {@link SchemaDTO}.
   *
   * @param schema The schema.
   * @return The schema DTO.
   */
  public static SchemaDTO toDTO(Schema schema) {
    return SchemaDTO.builder()
        .withName(schema.name())
        .withComment(schema.comment())
        .withProperties(schema.properties())
        .withAudit(toDTO(schema.auditInfo()))
        .build();
  }

  /**
   * Converts a {@link Column} to a {@link ColumnDTO}.
   *
   * @param column The column.
   * @return The column DTO.
   */
  public static ColumnDTO toDTO(Column column) {
    return ColumnDTO.builder()
        .withName(column.name())
        .withDataType(column.dataType())
        .withComment(column.comment())
        .withNullable(column.nullable())
        .withAutoIncrement(column.autoIncrement())
        .withDefaultValue(
            (column.defaultValue() == null
                    || column.defaultValue().equals(Column.DEFAULT_VALUE_NOT_SET))
                ? Column.DEFAULT_VALUE_NOT_SET
                : toFunctionArg(column.defaultValue()))
        .build();
  }

  /**
   * Converts a table implementation to a {@link TableDTO}.
   *
   * @param table The table implementation.
   * @return The table DTO.
   */
  public static TableDTO toDTO(Table table) {
    return TableDTO.builder()
        .withName(table.name())
        .withComment(table.comment())
        .withColumns(
            Arrays.stream(table.columns()).map(DTOConverters::toDTO).toArray(ColumnDTO[]::new))
        .withProperties(table.properties())
        .withSortOrders(toDTOs(table.sortOrder()))
        .withDistribution(toDTO(table.distribution()))
        .withAudit(toDTO(table.auditInfo()))
        .withPartitioning(toDTOs(table.partitioning()))
        .withIndex(toDTOs(table.index()))
        .build();
  }

  /**
   * Converts a Distribution implementation to a DistributionDTO.
   *
   * @param distribution The distribution implementation.
   * @return The distribution DTO.
   */
  public static DistributionDTO toDTO(Distribution distribution) {
    if (Distributions.NONE.equals(distribution) || null == distribution) {
      return DistributionDTO.NONE;
    }

    if (distribution instanceof DistributionDTO) {
      return (DistributionDTO) distribution;
    }

    return DistributionDTO.builder()
        .withStrategy(distribution.strategy())
        .withNumber(distribution.number())
        .withArgs(
            Arrays.stream(distribution.expressions())
                .map(DTOConverters::toFunctionArg)
                .toArray(FunctionArg[]::new))
        .build();
  }

  /**
   * Converts a SortOrder implementation to a SortOrderDTO.
   *
   * @param sortOrder The sort order implementation.
   * @return The sort order DTO.
   */
  public static SortOrderDTO toDTO(SortOrder sortOrder) {
    if (sortOrder instanceof SortOrderDTO) {
      return (SortOrderDTO) sortOrder;
    }

    return SortOrderDTO.builder()
        .withSortTerm(toFunctionArg(sortOrder.expression()))
        .withDirection(sortOrder.direction())
        .withNullOrder(sortOrder.nullOrdering())
        .build();
  }

  /**
   * Converts a Transform implementation to a Partitioning DTO.
   *
   * @param transform The transform implementation.
   * @return The partitioning DTO.
   */
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
      Transforms.ListTransform listTransform = (Transforms.ListTransform) transform;
      ListPartitionDTO[] assignments =
          Arrays.stream(listTransform.assignments())
              .map(DTOConverters::toDTO)
              .toArray(ListPartitionDTO[]::new);
      return ListPartitioningDTO.of(listTransform.fieldNames(), assignments);

    } else if (transform instanceof Transforms.RangeTransform) {
      Transforms.RangeTransform rangeTransform = (Transforms.RangeTransform) transform;
      RangePartitionDTO[] assignments =
          Arrays.stream(rangeTransform.assignments())
              .map(DTOConverters::toDTO)
              .toArray(RangePartitionDTO[]::new);
      return RangePartitioningDTO.of(rangeTransform.fieldName(), assignments);

    } else if (transform instanceof Transforms.ApplyTransform) {
      return FunctionPartitioningDTO.of(transform.name(), toFunctionArg(transform.arguments()));

    } else {
      throw new IllegalArgumentException("Unsupported transform: " + transform.name());
    }
  }

  /**
   * Converts an index implementation to an IndexDTO.
   *
   * @param index The index implementation.
   * @return The index DTO.
   */
  public static IndexDTO toDTO(Index index) {
    if (index instanceof IndexDTO) {
      return (IndexDTO) index;
    }
    return IndexDTO.builder()
        .withIndexType(index.type())
        .withName(index.name())
        .withFieldNames(index.fieldNames())
        .build();
  }

  /**
   * Converts a user implementation to a UserDTO.
   *
   * @param user The user implementation.
   * @return The user DTO.
   */
  public static UserDTO toDTO(User user) {
    if (user instanceof UserDTO) {
      return (UserDTO) user;
    }

    return UserDTO.builder()
        .withName(user.name())
        .withRoles(user.roles())
        .withAudit(toDTO(user.auditInfo()))
        .build();
  }

  /**
   * Converts a group implementation to a GroupDTO.
   *
   * @param group The group implementation.
   * @return The group DTO.
   */
  public static GroupDTO toDTO(Group group) {
    if (group instanceof GroupDTO) {
      return (GroupDTO) group;
    }

    return GroupDTO.builder()
        .withName(group.name())
        .withRoles(group.roles())
        .withAudit(toDTO(group.auditInfo()))
        .build();
  }

  /**
   * Converts a role implementation to a RoleDTO.
   *
   * @param role The role implementation.
   * @return The role DTO.
   */
  public static RoleDTO toDTO(Role role) {
    if (role instanceof RoleDTO) {
      return (RoleDTO) role;
    }

    return RoleDTO.builder()
        .withName(role.name())
        .withSecurableObjects(
            role.securableObjects().stream()
                .map(DTOConverters::toDTO)
                .toArray(SecurableObjectDTO[]::new))
        .withProperties(role.properties())
        .withAudit(toDTO(role.auditInfo()))
        .build();
  }

  /**
   * Converts a securable object implementation to a SecurableObjectDTO.
   *
   * @param securableObject The securable object implementation.
   * @return The securable object DTO.
   */
  public static SecurableObjectDTO toDTO(SecurableObject securableObject) {
    if (securableObject instanceof SecurableObjectDTO) {
      return (SecurableObjectDTO) securableObject;
    }

    return SecurableObjectDTO.builder()
        .withFullName(securableObject.fullName())
        .withType(securableObject.type())
        .withPrivileges(
            securableObject.privileges().stream()
                .map(DTOConverters::toDTO)
                .toArray(PrivilegeDTO[]::new))
        .build();
  }

  /**
   * Converts a privilege implementation to a PrivilegeDTO.
   *
   * @param privilege The privilege implementation.
   * @return The privilege DTO.
   */
  public static PrivilegeDTO toDTO(Privilege privilege) {
    if (privilege instanceof PrivilegeDTO) {
      return (PrivilegeDTO) privilege;
    }

    return PrivilegeDTO.builder()
        .withName(privilege.name())
        .withCondition(privilege.condition())
        .build();
  }

  /**
   * Converts a MetadataObject to a MetadataObjectDTO.
   *
   * @param metadataObject The metadata object to be converted.
   * @return The metadata object DTO.
   */
  public static MetadataObjectDTO toDTO(MetadataObject metadataObject) {
    return MetadataObjectDTO.builder()
        .withParent(metadataObject.parent())
        .withName(metadataObject.name())
        .withType(metadataObject.type())
        .build();
  }

  /**
   * Converts a Tag to a TagDTO.
   *
   * @param tag The tag to be converted.
   * @param inherited The inherited flag.
   * @return The tag DTO.
   */
  public static TagDTO toDTO(Tag tag, Optional<Boolean> inherited) {
    TagDTO.Builder builder =
        TagDTO.builder()
            .withName(tag.name())
            .withComment(tag.comment())
            .withProperties(tag.properties())
            .withAudit(toDTO(tag.auditInfo()))
            .withInherited(inherited);

    return builder.build();
  }

  /**
   * Converts an Expression to an FunctionArg DTO.
   *
   * @param expression The expression to be converted.
   * @return The expression DTO.
   */
  public static FunctionArg toFunctionArg(Expression expression) {
    if (expression instanceof FunctionArg) {
      return (FunctionArg) expression;
    }

    if (expression instanceof Literal) {
      if (Literals.NULL.equals(expression)) {
        return LiteralDTO.NULL;
      }
      return LiteralDTO.builder()
          .withValue((((Literal) expression).value().toString()))
          .withDataType(((Literal) expression).dataType())
          .build();
    } else if (expression instanceof NamedReference.FieldReference) {
      return FieldReferenceDTO.builder()
          .withFieldName(((NamedReference.FieldReference) expression).fieldName())
          .build();
    } else if (expression instanceof FunctionExpression) {
      return FuncExpressionDTO.builder()
          .withFunctionName(((FunctionExpression) expression).functionName())
          .withFunctionArgs(
              Arrays.stream(((FunctionExpression) expression).arguments())
                  .map(DTOConverters::toFunctionArg)
                  .toArray(FunctionArg[]::new))
          .build();
    } else if (expression instanceof UnparsedExpression) {
      return UnparsedExpressionDTO.builder()
          .withUnparsedExpression(((UnparsedExpression) expression).unparsedExpression())
          .build();
    } else {
      throw new IllegalArgumentException("Unsupported expression type: " + expression.getClass());
    }
  }

  /**
   * Converts an array of Expressions to an array of FunctionArg DTOs.
   *
   * @param expressions The expressions to be converted.
   * @return The array of FunctionArg DTOs.
   */
  public static FunctionArg[] toFunctionArg(Expression[] expressions) {
    if (ArrayUtils.isEmpty(expressions)) {
      return FunctionArg.EMPTY_ARGS;
    }
    return Arrays.stream(expressions).map(DTOConverters::toFunctionArg).toArray(FunctionArg[]::new);
  }

  /**
   * Converts a Fileset to a FilesetDTO.
   *
   * @param fileset The fileset to be converted.
   * @return The fileset DTO.
   */
  public static FilesetDTO toDTO(Fileset fileset) {
    return FilesetDTO.builder()
        .name(fileset.name())
        .comment(fileset.comment())
        .type(fileset.type())
        .storageLocation(fileset.storageLocation())
        .properties(fileset.properties())
        .audit(toDTO(fileset.auditInfo()))
        .build();
  }

  /**
   * Converts to a FilesetContextDTO.
   *
   * @param context The fileset context to be converted.
   * @return The fileset context DTO.
   */
  public static FilesetContextDTO toDTO(FilesetContext context) {
    return FilesetContextDTO.builder()
        .fileset(toDTO(context.fileset()))
        .actualPath(context.actualPath())
        .build();
  }

  /**
   * Converts a Topic to a TopicDTO.
   *
   * @param topic The topic to be converted.
   * @return The topic DTO.
   */
  public static TopicDTO toDTO(Topic topic) {
    return TopicDTO.builder()
        .withName(topic.name())
        .withComment(topic.comment())
        .withProperties(topic.properties())
        .withAudit(toDTO(topic.auditInfo()))
        .build();
  }

  /**
   * Converts an array of Columns to an array of ColumnDTOs.
   *
   * @param columns The columns to be converted.
   * @return The array of ColumnDTOs.
   */
  public static ColumnDTO[] toDTOs(Column[] columns) {
    if (ArrayUtils.isEmpty(columns)) {
      return new ColumnDTO[0];
    }
    return Arrays.stream(columns).map(DTOConverters::toDTO).toArray(ColumnDTO[]::new);
  }

  /**
   * Converts an array of SortOrders to an array of SortOrderDTOs.
   *
   * @param sortOrders The sort orders to be converted.
   * @return The array of SortOrderDTOs.
   */
  public static SortOrderDTO[] toDTOs(SortOrder[] sortOrders) {
    if (ArrayUtils.isEmpty(sortOrders)) {
      return new SortOrderDTO[0];
    }
    return Arrays.stream(sortOrders).map(DTOConverters::toDTO).toArray(SortOrderDTO[]::new);
  }

  /**
   * Converts an array of Transforms to an array of Partitioning DTOs.
   *
   * @param transforms The transforms to be converted.
   * @return The array of Partitioning DTOs.
   */
  public static Partitioning[] toDTOs(Transform[] transforms) {
    if (ArrayUtils.isEmpty(transforms)) {
      return new Partitioning[0];
    }
    return Arrays.stream(transforms).map(DTOConverters::toDTO).toArray(Partitioning[]::new);
  }

  /**
   * Converts an array of Indexes to an array of IndexDTOs.
   *
   * @param indexes The indexes to be converted.
   * @return The array of IndexDTOs.
   */
  public static IndexDTO[] toDTOs(Index[] indexes) {
    if (ArrayUtils.isEmpty(indexes)) {
      return IndexDTO.EMPTY_INDEXES;
    }
    return Arrays.stream(indexes).map(DTOConverters::toDTO).toArray(IndexDTO[]::new);
  }

  /**
   * Converts an array of Partitions to an array of PartitionDTOs.
   *
   * @param partitions The partitions to be converted.
   * @return The array of PartitionDTOs.
   */
  public static PartitionDTO[] toDTOs(Partition[] partitions) {
    if (ArrayUtils.isEmpty(partitions)) {
      return new PartitionDTO[0];
    }
    return Arrays.stream(partitions).map(DTOConverters::toDTO).toArray(PartitionDTO[]::new);
  }

  /**
   * Converts an array of Catalogs to an array of CatalogDTOs.
   *
   * @param catalogs The catalogs to be converted.
   * @return The array of CatalogDTOs.
   */
  public static CatalogDTO[] toDTOs(Catalog[] catalogs) {
    if (ArrayUtils.isEmpty(catalogs)) {
      return new CatalogDTO[0];
    }
    return Arrays.stream(catalogs).map(DTOConverters::toDTO).toArray(CatalogDTO[]::new);
  }

  /**
   * Converts a DistributionDTO to a Distribution.
   *
   * @param distributionDTO The distribution DTO.
   * @return The distribution.
   */
  public static Distribution fromDTO(DistributionDTO distributionDTO) {
    if (DistributionDTO.NONE.equals(distributionDTO) || null == distributionDTO) {
      return Distributions.NONE;
    }

    return Distributions.of(
        distributionDTO.strategy(),
        distributionDTO.number(),
        fromFunctionArgs(distributionDTO.args()));
  }

  /**
   * Converts a FunctionArg DTO to an Expression.
   *
   * @param args The function argument DTOs to be converted.
   * @return The array of Expressions.
   */
  public static Expression[] fromFunctionArgs(FunctionArg[] args) {
    if (ArrayUtils.isEmpty(args)) {
      return Expression.EMPTY_EXPRESSION;
    }
    return Arrays.stream(args).map(DTOConverters::fromFunctionArg).toArray(Expression[]::new);
  }

  /**
   * Converts a FunctionArg DTO to an Expression.
   *
   * @param arg The function argument DTO to be converted.
   * @return The expression.
   */
  public static Expression fromFunctionArg(FunctionArg arg) {
    switch (arg.argType()) {
      case LITERAL:
        if (((LiteralDTO) arg).value() == null
            || ((LiteralDTO) arg).dataType().equals(Types.NullType.get())) {
          return Literals.NULL;
        }
        return Literals.of(((LiteralDTO) arg).value(), ((LiteralDTO) arg).dataType());
      case FIELD:
        return NamedReference.field(((FieldReferenceDTO) arg).fieldName());
      case FUNCTION:
        return FunctionExpression.of(
            ((FuncExpressionDTO) arg).functionName(),
            fromFunctionArgs(((FuncExpressionDTO) arg).args()));
      case UNPARSED:
        return UnparsedExpression.of(((UnparsedExpressionDTO) arg).unparsedExpression());
      default:
        throw new IllegalArgumentException("Unsupported expression type: " + arg.getClass());
    }
  }

  /**
   * Converts a IndexDTO to an Index.
   *
   * @param indexDTO The Index DTO to be converted.
   * @return The index.
   */
  public static Index fromDTO(IndexDTO indexDTO) {
    return Indexes.of(indexDTO.type(), indexDTO.name(), indexDTO.fieldNames());
  }

  /**
   * Converts an array of IndexDTOs to an array of Indexes.
   *
   * @param indexDTOs The Index DTOs to be converted.
   * @return The array of Indexes.
   */
  public static Index[] fromDTOs(IndexDTO[] indexDTOs) {
    if (ArrayUtils.isEmpty(indexDTOs)) {
      return Indexes.EMPTY_INDEXES;
    }

    return Arrays.stream(indexDTOs).map(DTOConverters::fromDTO).toArray(Index[]::new);
  }

  /**
   * Converts a PartitionDTO to a Partition.
   *
   * @param partitionDTO The partition DTO to be converted.
   * @return The partition.
   */
  public static Partition fromDTO(PartitionDTO partitionDTO) {
    switch (partitionDTO.type()) {
      case IDENTITY:
        IdentityPartitionDTO identityPartitionDTO = (IdentityPartitionDTO) partitionDTO;
        Literal<?>[] values =
            Arrays.stream(identityPartitionDTO.values())
                .map(DTOConverters::fromFunctionArg)
                .toArray(Literal<?>[]::new);
        return Partitions.identity(
            identityPartitionDTO.name(),
            identityPartitionDTO.fieldNames(),
            values,
            identityPartitionDTO.properties());
      case RANGE:
        RangePartitionDTO rangePartitionDTO = (RangePartitionDTO) partitionDTO;
        return Partitions.range(
            rangePartitionDTO.name(),
            (Literal<?>) fromFunctionArg(rangePartitionDTO.upper()),
            (Literal<?>) fromFunctionArg(rangePartitionDTO.lower()),
            rangePartitionDTO.properties());
      case LIST:
        ListPartitionDTO listPartitionDTO = (ListPartitionDTO) partitionDTO;
        Literal<?>[][] lists =
            Arrays.stream(listPartitionDTO.lists())
                .map(
                    list ->
                        Arrays.stream(list)
                            .map(DTOConverters::fromFunctionArg)
                            .toArray(Literal<?>[]::new))
                .toArray(Literal<?>[][]::new);
        return Partitions.list(listPartitionDTO.name(), lists, listPartitionDTO.properties());
      default:
        throw new IllegalArgumentException("Unsupported partition type: " + partitionDTO.type());
    }
  }

  /**
   * Converts a SortOrderDTO to a SortOrder.
   *
   * @param sortOrderDTO The sort order DTO to be converted.
   * @return The sort order.
   */
  public static SortOrder fromDTO(SortOrderDTO sortOrderDTO) {
    return SortOrders.of(
        fromFunctionArg(sortOrderDTO.sortTerm()),
        sortOrderDTO.direction(),
        sortOrderDTO.nullOrdering());
  }

  /**
   * Converts an array of SortOrderDTOs to an array of SortOrders.
   *
   * @param sortOrderDTO The sort order DTOs to be converted.
   * @return The array of SortOrders.
   */
  public static SortOrder[] fromDTOs(SortOrderDTO[] sortOrderDTO) {
    if (ArrayUtils.isEmpty(sortOrderDTO)) {
      return new SortOrder[0];
    }

    return Arrays.stream(sortOrderDTO).map(DTOConverters::fromDTO).toArray(SortOrder[]::new);
  }

  /**
   * Converts an array of Partitioning DTOs to an array of Transforms.
   *
   * @param partitioning The partitioning DTOs to be converted.
   * @return The array of Transforms.
   */
  public static Transform[] fromDTOs(Partitioning[] partitioning) {
    if (ArrayUtils.isEmpty(partitioning)) {
      return new Transform[0];
    }
    return Arrays.stream(partitioning).map(DTOConverters::fromDTO).toArray(Transform[]::new);
  }

  /**
   * Converts a ColumnDTO to a Column.
   *
   * @param columns The column DTO to be converted.
   * @return The column.
   */
  public static Column[] fromDTOs(ColumnDTO[] columns) {
    if (ArrayUtils.isEmpty(columns)) {
      return new Column[0];
    }
    return Arrays.stream(columns).map(DTOConverters::fromDTO).toArray(Column[]::new);
  }

  /**
   * Converts a ColumnDTO to a Column.
   *
   * @param column The column DTO to be converted.
   * @return The column.
   */
  public static Column fromDTO(ColumnDTO column) {
    if (column.defaultValue().equals(Column.DEFAULT_VALUE_NOT_SET)) {
      return column;
    }
    return Column.of(
        column.name(),
        column.dataType(),
        column.comment(),
        column.nullable(),
        column.autoIncrement(),
        fromFunctionArg((FunctionArg) column.defaultValue()));
  }

  /**
   * Converts a TableDTO to a Table.
   *
   * @param tableDTO The table DTO to be converted.
   * @return The table.
   */
  public static Table fromDTO(TableDTO tableDTO) {
    return new Table() {
      @Override
      public String name() {
        return tableDTO.name();
      }

      @Override
      public Column[] columns() {
        return fromDTOs((ColumnDTO[]) tableDTO.columns());
      }

      @Override
      public Transform[] partitioning() {
        return fromDTOs((Partitioning[]) tableDTO.partitioning());
      }

      @Override
      public SortOrder[] sortOrder() {
        return fromDTOs((SortOrderDTO[]) tableDTO.sortOrder());
      }

      @Override
      public Distribution distribution() {
        return fromDTO((DistributionDTO) tableDTO.distribution());
      }

      @Override
      public Index[] index() {
        return fromDTOs((IndexDTO[]) tableDTO.index());
      }

      @Override
      public String comment() {
        return tableDTO.comment();
      }

      @Override
      public Map<String, String> properties() {
        return tableDTO.properties();
      }

      @Override
      public Audit auditInfo() {
        return tableDTO.auditInfo();
      }
    };
  }

  /**
   * Converts a partitioning DTO to a Transform.
   *
   * @param partitioning The partitioning DTO to be converted.
   * @return The transform.
   */
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
        ListPartitioningDTO listPartitioningDTO = (ListPartitioningDTO) partitioning;
        ListPartition[] listPartitions =
            Arrays.stream(listPartitioningDTO.assignments())
                .map(p -> (ListPartition) fromDTO(p))
                .toArray(ListPartition[]::new);
        return Transforms.list(listPartitioningDTO.fieldNames(), listPartitions);
      case RANGE:
        RangePartitioningDTO rangePartitioningDTO = (RangePartitioningDTO) partitioning;
        RangePartition[] rangePartitions =
            Arrays.stream(rangePartitioningDTO.assignments())
                .map(p -> (RangePartition) fromDTO(p))
                .toArray(RangePartition[]::new);
        return Transforms.range(rangePartitioningDTO.fieldName(), rangePartitions);
      case FUNCTION:
        return Transforms.apply(
            ((FunctionPartitioningDTO) partitioning).functionName(),
            fromFunctionArgs(((FunctionPartitioningDTO) partitioning).args()));
      default:
        throw new IllegalArgumentException("Unsupported partitioning: " + partitioning.strategy());
    }
  }
}
