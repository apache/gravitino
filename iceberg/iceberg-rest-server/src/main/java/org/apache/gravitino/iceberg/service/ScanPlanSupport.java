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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
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
   * Returns a branch that points at the snapshot, preferring {@code main}.
   *
   * <p>Iceberg's REST client sends {@code use-snapshot-schema=false} for a branch scan, and {@code
   * TableScan#useRef} on a branch reads its snapshot with the current table schema, which is what
   * that flag asks for.
   */
  static Optional<String> branchAt(Table table, long snapshotId) {
    SnapshotRef main = table.refs().get(SnapshotRef.MAIN_BRANCH);
    if (main != null && main.snapshotId() == snapshotId) {
      return Optional.of(SnapshotRef.MAIN_BRANCH);
    }
    return table.refs().entrySet().stream()
        .filter(entry -> entry.getValue().isBranch() && entry.getValue().snapshotId() == snapshotId)
        .map(Map.Entry::getKey)
        .sorted()
        .findFirst();
  }

  /**
   * Maps column names of the current table schema to the names the same fields have in an older
   * snapshot schema, so a scan pinned to that snapshot can serve a request written against the
   * current schema. Names are resolved on the current schema first, so a name that only the
   * snapshot schema has, or that a later column reuses, never binds to the snapshot's field.
   */
  static final class SnapshotNames {
    private final Schema current;
    private final Schema snapshot;
    private final boolean caseSensitive;
    private final long snapshotId;

    SnapshotNames(Schema current, Schema snapshot, boolean caseSensitive, long snapshotId) {
      this.current = current;
      this.snapshot = snapshot;
      this.caseSensitive = caseSensitive;
      this.snapshotId = snapshotId;
    }

    /**
     * Returns the snapshot schema's name for a current-schema column.
     *
     * @param name the column name in the current table schema
     * @return the column name in the snapshot schema, or {@code null} if the column was added after
     *     the snapshot
     * @throws IllegalArgumentException if the current table schema has no such column
     */
    @Nullable
    String toSnapshotName(String name) {
      Types.NestedField field =
          caseSensitive ? current.findField(name) : current.caseInsensitiveFindField(name);
      if (field == null) {
        throw new IllegalArgumentException(
            String.format("Cannot find field '%s' in the current table schema", name));
      }
      return snapshot.findColumnName(field.fieldId());
    }

    long snapshotId() {
      return snapshotId;
    }
  }

  /**
   * Rewrites unbound filter terms from current-schema names to snapshot-schema names.
   *
   * @throws IllegalArgumentException if a term names a column the current schema lacks, or a column
   *     added after the snapshot, whose values the snapshot's files do not contain
   */
  static Expression rewriteFilterNames(Expression filter, SnapshotNames names) {
    if (filter == null) {
      return null;
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
            String name = pred.ref().name();
            String snapshotName = names.toSnapshotName(name);
            if (snapshotName == null) {
              throw new IllegalArgumentException(
                  String.format(
                      "Cannot filter on field '%s': it does not exist in snapshot %s",
                      name, names.snapshotId()));
            }
            if (snapshotName.equals(name)) {
              return pred;
            }
            UnboundTerm<T> term = renameTerm(pred.term(), snapshotName);
            if (pred.literals() == null) {
              return Expressions.predicate(pred.op(), term);
            }
            List<T> values =
                pred.literals().stream().map(Literal::value).collect(Collectors.toList());
            return Expressions.predicate(pred.op(), term, values);
          }
        });
  }

  /**
   * Rewrites projected or statistics column names from the current schema to the snapshot schema.
   * Columns added after the snapshot are dropped: none of the snapshot's files contain them.
   *
   * @throws IllegalArgumentException if the current table schema has no such column
   */
  static List<String> rewriteColumnNames(List<String> columns, SnapshotNames names) {
    List<String> rewritten = Lists.newArrayListWithCapacity(columns.size());
    for (String column : columns) {
      String snapshotName = names.toSnapshotName(column);
      if (snapshotName != null) {
        rewritten.add(snapshotName);
      }
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
  private static <T> UnboundTerm<T> renameTerm(UnboundTerm<T> term, String name) {
    if (term instanceof UnboundTransform) {
      return Expressions.transform(name, ((UnboundTransform<?, T>) term).transform());
    }
    return (UnboundTerm<T>) Expressions.ref(name);
  }
}
