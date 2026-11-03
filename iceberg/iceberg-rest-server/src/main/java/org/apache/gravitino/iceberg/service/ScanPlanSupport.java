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
package org.apache.gravitino.iceberg.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.NamedReference;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.expressions.UnboundTransform;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Helpers for Iceberg REST server-side scan planning: honour {@code use-snapshot-schema} and avoid
 * failing when historical partition specs reference dropped columns.
 */
final class ScanPlanSupport {

  private ScanPlanSupport() {}

  /**
   * Builds a map from current-schema field names to the same field's name in the snapshot schema.
   * Only entries whose names differ are included.
   */
  @VisibleForTesting
  static Map<String, String> currentNameToSnapshotName(Schema current, Schema snapshot) {
    Map<String, String> mapping = new HashMap<>();
    Map<Integer, String> snapshotNames = snapshot.idToName();
    for (Map.Entry<Integer, String> entry : current.idToName().entrySet()) {
      String snapshotName = snapshotNames.get(entry.getKey());
      if (snapshotName != null && !snapshotName.equals(entry.getValue())) {
        mapping.put(entry.getValue(), snapshotName);
      }
    }
    return Collections.unmodifiableMap(mapping);
  }

  /**
   * Rewrites unbound filter terms that use current-schema names so they bind against a snapshot
   * schema after {@code TableScan#useSnapshot}.
   */
  @VisibleForTesting
  static Expression rewriteFilterNames(Expression filter, Map<String, String> currentToSnapshot) {
    if (filter == null || currentToSnapshot.isEmpty()) {
      return filter;
    }
    return ExpressionVisitors.visit(
        filter,
        new ExpressionVisitors.ExpressionVisitor<Expression>() {
          @Override
          public Expression alwaysTrue() {
            return Expressions.alwaysTrue();
          }

          @Override
          public Expression alwaysFalse() {
            return Expressions.alwaysFalse();
          }

          @Override
          public Expression not(Expression result) {
            return Expressions.not(result);
          }

          @Override
          public Expression and(Expression left, Expression right) {
            return Expressions.and(left, right);
          }

          @Override
          public Expression or(Expression left, Expression right) {
            return Expressions.or(left, right);
          }

          @Override
          public <T> Expression predicate(UnboundPredicate<T> pred) {
            UnboundTerm<T> rewritten = rewriteTerm(pred.term(), currentToSnapshot);
            if (rewritten == pred.term()) {
              return pred;
            }
            return recreatePredicate(pred, rewritten);
          }
        });
  }

  @SuppressWarnings("unchecked")
  private static <T> Expression recreatePredicate(
      UnboundPredicate<T> pred, UnboundTerm<T> rewritten) {
    if (pred.literals() != null && !pred.literals().isEmpty()) {
      List<T> values = pred.literals().stream().map(l -> l.value()).collect(Collectors.toList());
      return Expressions.predicate(pred.op(), rewritten, values);
    }
    if (pred.literal() != null) {
      T value = pred.literal().value();
      switch (pred.op()) {
        case LT:
          return Expressions.lessThan(rewritten, value);
        case LT_EQ:
          return Expressions.lessThanOrEqual(rewritten, value);
        case GT:
          return Expressions.greaterThan(rewritten, value);
        case GT_EQ:
          return Expressions.greaterThanOrEqual(rewritten, value);
        case EQ:
          return Expressions.equal(rewritten, value);
        case NOT_EQ:
          return Expressions.notEqual(rewritten, value);
        case STARTS_WITH:
          return Expressions.startsWith((UnboundTerm<String>) rewritten, (String) value);
        case NOT_STARTS_WITH:
          return Expressions.notStartsWith((UnboundTerm<String>) rewritten, (String) value);
        default:
          if (rewritten instanceof NamedReference) {
            return Expressions.predicate(
                pred.op(), ((NamedReference<?>) rewritten).name(), pred.literal());
          }
          return pred;
      }
    }
    switch (pred.op()) {
      case IS_NULL:
        return Expressions.isNull(rewritten);
      case NOT_NULL:
        return Expressions.notNull(rewritten);
      case IS_NAN:
        return Expressions.isNaN(rewritten);
      case NOT_NAN:
        return Expressions.notNaN(rewritten);
      default:
        return Expressions.predicate(pred.op(), rewritten);
    }
  }

  /** Rewrites projected column names from the current schema to the snapshot schema by field id. */
  @VisibleForTesting
  static List<String> rewriteColumnNames(
      List<String> columns, Map<String, String> currentToSnapshot) {
    if (columns == null || columns.isEmpty() || currentToSnapshot.isEmpty()) {
      return columns;
    }
    List<String> rewritten = Lists.newArrayListWithCapacity(columns.size());
    for (String column : columns) {
      rewritten.add(currentToSnapshot.getOrDefault(column, column));
    }
    return rewritten;
  }

  /**
   * Rebinds partition specs whose {@link PartitionSpec#partitionType()} contains {@code unknown}
   * (dropped source columns) so file-scan-task serialization can write partition values.
   */
  @VisibleForTesting
  static Map<Integer, PartitionSpec> sanitizeSpecsForSerialization(
      Map<Integer, PartitionSpec> specsById, List<FileScanTask> fileScanTasks) {
    Map<Integer, Types.StructType> partitionTypesBySpecId = Maps.newHashMap();
    for (FileScanTask task : fileScanTasks) {
      StructLike partition = task.file().partition();
      if (partition instanceof PartitionData) {
        partitionTypesBySpecId.putIfAbsent(
            task.file().specId(), ((PartitionData) partition).getPartitionType());
      }
    }

    Map<Integer, PartitionSpec> sanitized = Maps.newHashMap();
    boolean changed = false;
    for (Map.Entry<Integer, PartitionSpec> entry : specsById.entrySet()) {
      PartitionSpec spec = entry.getValue();
      if (!partitionTypeHasUnknown(spec.partitionType())) {
        sanitized.put(entry.getKey(), spec);
        continue;
      }
      Schema extended =
          extendSchemaForDroppedSources(spec, partitionTypesBySpecId.get(entry.getKey()));
      // ignoreMissingFields covers any residual gaps if PartitionData did not supply a type.
      sanitized.put(entry.getKey(), spec.toUnbound().bind(extended, true));
      changed = true;
    }
    return changed ? Collections.unmodifiableMap(sanitized) : specsById;
  }

  private static boolean partitionTypeHasUnknown(Types.StructType partitionType) {
    for (Types.NestedField field : partitionType.fields()) {
      if (field.type().typeId() == Type.TypeID.UNKNOWN) {
        return true;
      }
    }
    return false;
  }

  private static Schema extendSchemaForDroppedSources(
      PartitionSpec spec, Types.StructType partitionDataType) {
    Schema schema = spec.schema();
    List<Types.NestedField> extras = Lists.newArrayList();
    List<Types.NestedField> partitionFields =
        partitionDataType == null ? Collections.emptyList() : partitionDataType.fields();
    int partitionPos = 0;
    for (PartitionField field : spec.fields()) {
      Types.NestedField source = schema.findField(field.sourceId());
      Type partitionFieldType =
          partitionPos < partitionFields.size() ? partitionFields.get(partitionPos).type() : null;
      partitionPos++;
      if (source != null) {
        continue;
      }
      Type restored =
          partitionFieldType != null && partitionFieldType.typeId() != Type.TypeID.UNKNOWN
              ? partitionFieldType
              : Types.BinaryType.get();
      extras.add(
          Types.NestedField.optional(
              field.sourceId(), "_dropped_source_" + field.sourceId(), restored));
    }
    if (extras.isEmpty()) {
      return schema;
    }
    List<Types.NestedField> all = Lists.newArrayList(schema.columns());
    all.addAll(extras);
    return new Schema(all);
  }

  @SuppressWarnings("unchecked")
  private static <T> UnboundTerm<T> rewriteTerm(
      UnboundTerm<T> term, Map<String, String> currentToSnapshot) {
    if (term instanceof NamedReference) {
      String name = ((NamedReference<?>) term).name();
      String mapped = currentToSnapshot.get(name);
      if (mapped == null || mapped.equals(name)) {
        return term;
      }
      return (UnboundTerm<T>) Expressions.ref(mapped);
    }
    if (term instanceof UnboundTransform) {
      UnboundTransform<?, T> transform = (UnboundTransform<?, T>) term;
      NamedReference<?> ref = transform.ref();
      String mapped = currentToSnapshot.get(ref.name());
      if (mapped == null || mapped.equals(ref.name())) {
        return term;
      }
      // Recreate common transforms with the snapshot field name.
      String transformName = transform.transform().toString();
      if (transformName.startsWith("bucket[")) {
        int width =
            Integer.parseInt(
                transformName.substring("bucket[".length(), transformName.length() - 1));
        return (UnboundTerm<T>) Expressions.bucket(mapped, width);
      }
      if (transformName.startsWith("truncate[")) {
        int width =
            Integer.parseInt(
                transformName.substring("truncate[".length(), transformName.length() - 1));
        return (UnboundTerm<T>) Expressions.truncate(mapped, width);
      }
      switch (transformName) {
        case "year":
          return (UnboundTerm<T>) Expressions.year(mapped);
        case "month":
          return (UnboundTerm<T>) Expressions.month(mapped);
        case "day":
          return (UnboundTerm<T>) Expressions.day(mapped);
        case "hour":
          return (UnboundTerm<T>) Expressions.hour(mapped);
        default:
          return term;
      }
    }
    return term;
  }
}
