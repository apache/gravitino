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
import com.google.common.collect.Sets;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.expressions.UnboundTransform;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

/**
 * Helpers for Iceberg REST server-side scan planning: honour {@code use-snapshot-schema} and avoid
 * failing when historical partition specs reference dropped columns.
 */
final class IcebergScanPlanHelper {

  private IcebergScanPlanHelper() {}

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
   * Rewrites filter/select/stats field names from the current table schema to the snapshot schema
   * by field id, so a {@code useSnapshot} scan can serve a request written against the current
   * schema. Names are resolved on the current schema first, so a name that only the snapshot schema
   * has, or that a later column reuses, never binds to the snapshot's field. Columns added after
   * the snapshot are dropped from select/stats; filters on them are rewritten conservatively for
   * planning (never under-prune under OR/NOT) and the full original filter is reattached as a
   * residual by the caller.
   */
  static PlanTableScanRequest rewriteForSnapshot(
      PlanTableScanRequest request, Schema current, Schema snapshot) {
    PlanTableScanRequest.Builder builder =
        PlanTableScanRequest.builder()
            .withSnapshotId(request.snapshotId())
            .withCaseSensitive(request.caseSensitive())
            .withUseSnapshotSchema(request.useSnapshotSchema())
            .withStartSnapshotId(request.startSnapshotId())
            .withEndSnapshotId(request.endSnapshotId())
            .withMinRowsRequested(request.minRowsRequested());
    if (request.filter() != null) {
      builder.withFilter(
          rewriteFilterNames(request.filter(), current, snapshot, request.caseSensitive()));
    }
    if (request.select() != null && !request.select().isEmpty()) {
      List<String> select =
          rewriteColumnNames(request.select(), current, snapshot, request.caseSensitive());
      if (!select.isEmpty()) {
        builder.withSelect(select);
      }
    }
    if (request.statsFields() != null && !request.statsFields().isEmpty()) {
      List<String> statsFields =
          rewriteColumnNames(request.statsFields(), current, snapshot, request.caseSensitive());
      if (!statsFields.isEmpty()) {
        builder.withStatsFields(statsFields);
      }
    }
    return builder.build();
  }

  /**
   * Remaps residual filters that were bound/named against {@code from} onto {@code to} by field id,
   * so a response for {@code use-snapshot-schema=false} keeps the request schema's identities.
   *
   * <p>Handles both bound residuals (field id from the scan schema) and unbound ones (names
   * resolved on {@code from}).
   */
  @VisibleForTesting
  static Expression remapExpressionNames(
      Expression expression, Schema from, Schema to, boolean caseSensitive) {
    if (expression == null) {
      return null;
    }
    return ExpressionVisitors.visit(
        expression,
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
          public <T> Expression predicate(BoundPredicate<T> pred) {
            String toName = to.findColumnName(pred.ref().fieldId());
            if (toName == null) {
              return Expressions.alwaysTrue();
            }
            return unboundFromBound(pred, toName);
          }

          @Override
          public <T> Expression predicate(UnboundPredicate<T> pred) {
            String fromName = pred.ref().name();
            String toName = renameByFieldId(fromName, from, to, caseSensitive);
            if (toName == null) {
              // Field was dropped from the request schema; residual cannot name it.
              return Expressions.alwaysTrue();
            }
            if (toName.equals(fromName)) {
              return pred;
            }
            return unboundWithName(pred, toName);
          }
        });
  }

  /**
   * Remaps residuals (bound or unbound) onto {@code to} by field id. Bound residuals from a
   * snapshot scan carry snapshot names; unbound ones do as well after a name rewrite.
   */
  @VisibleForTesting
  static Expression remapResidualToSchema(Expression residual, Schema to, boolean caseSensitive) {
    if (residual == null) {
      return null;
    }
    return ExpressionVisitors.visit(
        residual,
        new ExpressionVisitors.BoundExpressionVisitor<Expression>() {
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
          public <T> Expression predicate(BoundPredicate<T> pred) {
            String toName = to.findColumnName(pred.ref().fieldId());
            if (toName == null) {
              return Expressions.alwaysTrue();
            }
            return unboundFromBound(pred, toName);
          }

          @Override
          public <T> Expression predicate(UnboundPredicate<T> pred) {
            // Unbound residual still carries the scan-schema name; resolve via {@code to} only when
            // the name already matches a current field, otherwise keep as-is (tests use unbound
            // remaps through {@link #remapExpressionNames}).
            Types.NestedField field =
                caseSensitive
                    ? to.findField(pred.ref().name())
                    : to.caseInsensitiveFindField(pred.ref().name());
            if (field != null) {
              return pred;
            }
            return pred;
          }
        });
  }

  /**
   * Prepares planned tasks for the REST response: remap residuals onto the request schema and
   * rewrite tasks whose partition spec still has {@code unknown} onto a client-parseable spec from
   * {@code tableSpecs} (prefer unpartitioned, else any spec without {@code unknown}) so Iceberg
   * clients can parse them with their local {@code table.specs()} map. Associated delete files are
   * rewritten to the same target spec.
   */
  static List<FileScanTask> prepareTasksForResponse(
      List<FileScanTask> fileScanTasks,
      Map<Integer, PartitionSpec> tableSpecs,
      @Nullable Schema residualTargetSchema,
      @Nullable Schema residualSourceSchema,
      @Nullable Expression extraResidual,
      boolean caseSensitive) {
    if (fileScanTasks.isEmpty()) {
      return fileScanTasks;
    }
    PartitionSpec compatibleSpec = findClientCompatibleSpec(tableSpecs);
    List<FileScanTask> prepared = Lists.newArrayListWithCapacity(fileScanTasks.size());
    for (FileScanTask task : fileScanTasks) {
      Expression residual = task.residual();
      if (residualTargetSchema != null && residual != null) {
        if (residualSourceSchema != null) {
          residual =
              remapExpressionNames(
                  residual, residualSourceSchema, residualTargetSchema, caseSensitive);
        } else {
          residual = remapResidualToSchema(residual, residualTargetSchema, caseSensitive);
        }
      }
      if (extraResidual != null) {
        residual = residual == null ? extraResidual : Expressions.and(residual, extraResidual);
      }
      PartitionSpec spec = tableSpecs.get(task.file().specId());
      boolean needsSpecRewrite =
          spec != null && partitionTypeHasUnknown(spec.partitionType()) && compatibleSpec != null;
      if (needsSpecRewrite) {
        prepared.add(
            withCompatibleSpec(
                task, compatibleSpec, residual, residualTargetSchema, caseSensitive));
      } else if (residual != null && residual != task.residual()) {
        prepared.add(withResidual(task, residual, residualTargetSchema, caseSensitive));
      } else {
        prepared.add(task);
      }
    }
    return prepared;
  }

  /**
   * Rebinds partition specs whose {@link PartitionSpec#partitionType()} contains {@code unknown}
   * (dropped source columns) so file-scan-task serialization can write partition values. Source
   * types are restored from historical table schemas (and identity partition data when present);
   * transform result types and a binary guess are never used. Specs that cannot be restored are
   * left unchanged so empty plans are not made worse.
   */
  @VisibleForTesting
  static Map<Integer, PartitionSpec> sanitizeSpecsForSerialization(
      Map<Integer, PartitionSpec> specsById,
      List<FileScanTask> fileScanTasks,
      Collection<Schema> historicalSchemas) {
    Map<Integer, Types.StructType> partitionTypesBySpecId = Maps.newHashMap();
    for (FileScanTask task : fileScanTasks) {
      StructLike partition = task.file().partition();
      if (partition instanceof PartitionData) {
        partitionTypesBySpecId.putIfAbsent(
            task.file().specId(), ((PartitionData) partition).getPartitionType());
      }
    }

    Map<Integer, Type> typesBySourceId = indexTypesByFieldId(historicalSchemas);

    Map<Integer, PartitionSpec> sanitized = Maps.newHashMap();
    boolean changed = false;
    for (Map.Entry<Integer, PartitionSpec> entry : specsById.entrySet()) {
      PartitionSpec spec = entry.getValue();
      if (!partitionTypeHasUnknown(spec.partitionType())) {
        sanitized.put(entry.getKey(), spec);
        continue;
      }
      Schema extended =
          extendSchemaForDroppedSources(
              spec, partitionTypesBySpecId.get(entry.getKey()), typesBySourceId);
      if (extended == spec.schema()) {
        // Could not restore every dropped source; leave the broken spec alone.
        sanitized.put(entry.getKey(), spec);
        continue;
      }
      // ignoreMissingFields covers any residual gaps if a type is still missing.
      sanitized.put(entry.getKey(), spec.toUnbound().bind(extended, true));
      changed = true;
    }
    return changed ? Collections.unmodifiableMap(sanitized) : specsById;
  }

  @VisibleForTesting
  static Expression rewriteFilterNames(
      Expression filter, Schema current, Schema snapshot, boolean caseSensitive) {
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
            // Post-snapshot predicates become always-true for planning. Negating that would yield
            // always-false and incorrectly prune every file (NOT(new_col = 1) on historical rows
            // is typically true). Keep planning conservative: do not under-include.
            if (result.op() == Expression.Operation.TRUE) {
              return Expressions.alwaysTrue();
            }
            if (result.op() == Expression.Operation.FALSE) {
              return Expressions.alwaysFalse();
            }
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
            String snapshotName = toSnapshotName(name, current, snapshot, caseSensitive);
            if (snapshotName == null) {
              // Column was added after the snapshot. Do not prune files here; the caller ANDs the
              // full original filter onto the response residual.
              return Expressions.alwaysTrue();
            }
            if (snapshotName.equals(name)) {
              return pred;
            }
            return unboundWithName(pred, snapshotName);
          }
        });
  }

  @VisibleForTesting
  static List<String> rewriteColumnNames(
      List<String> columns, Schema current, Schema snapshot, boolean caseSensitive) {
    List<String> rewritten = Lists.newArrayListWithCapacity(columns.size());
    for (String column : columns) {
      if ("*".equals(column)) {
        // Iceberg schema selection treats "*" as project-all, not a field name.
        rewritten.add("*");
        continue;
      }
      String snapshotName = toSnapshotName(column, current, snapshot, caseSensitive);
      if (snapshotName != null) {
        rewritten.add(snapshotName);
      }
    }
    return rewritten;
  }

  /**
   * Returns the snapshot schema's name for a current-schema column, or {@code null} if the column
   * was added after the snapshot.
   *
   * @throws IllegalArgumentException if the current table schema has no such column
   */
  @Nullable
  private static String toSnapshotName(
      String name, Schema current, Schema snapshot, boolean caseSensitive) {
    Types.NestedField field =
        caseSensitive ? current.findField(name) : current.caseInsensitiveFindField(name);
    if (field == null) {
      throw new IllegalArgumentException(
          String.format("Cannot find field '%s' in the current table schema", name));
    }
    return snapshot.findColumnName(field.fieldId());
  }

  @Nullable
  private static String renameByFieldId(
      String fromName, Schema from, Schema to, boolean caseSensitive) {
    Types.NestedField field =
        caseSensitive ? from.findField(fromName) : from.caseInsensitiveFindField(fromName);
    if (field == null) {
      return null;
    }
    return to.findColumnName(field.fieldId());
  }

  private static boolean partitionTypeHasUnknown(Types.StructType partitionType) {
    for (Types.NestedField field : partitionType.fields()) {
      if (field.type().typeId() == Type.TypeID.UNKNOWN) {
        return true;
      }
    }
    return false;
  }

  private static Map<Integer, Type> indexTypesByFieldId(Collection<Schema> schemas) {
    Map<Integer, Type> types = Maps.newHashMap();
    if (schemas == null) {
      return types;
    }
    for (Schema schema : schemas) {
      for (Types.NestedField field : TypeUtil.indexById(schema.asStruct()).values()) {
        types.putIfAbsent(field.fieldId(), field.type());
      }
    }
    return types;
  }

  private static Schema extendSchemaForDroppedSources(
      PartitionSpec spec, Types.StructType partitionDataType, Map<Integer, Type> typesBySourceId) {
    Schema schema = spec.schema();
    List<Types.NestedField> extras = Lists.newArrayList();
    Set<Integer> addedSourceIds = Sets.newHashSet();
    List<Types.NestedField> partitionFields =
        partitionDataType == null ? Collections.emptyList() : partitionDataType.fields();
    int partitionPos = 0;
    for (PartitionField field : spec.fields()) {
      Types.NestedField source = schema.findField(field.sourceId());
      Type partitionFieldType =
          partitionPos < partitionFields.size() ? partitionFields.get(partitionPos).type() : null;
      partitionPos++;
      if (source != null || !addedSourceIds.add(field.sourceId())) {
        continue;
      }
      Type restored = typesBySourceId.get(field.sourceId());
      if (restored == null
          && field.transform().isIdentity()
          && partitionFieldType != null
          && partitionFieldType.typeId() != Type.TypeID.UNKNOWN) {
        // Identity partition values share the source type; non-identity transform outputs do not.
        restored = partitionFieldType;
      }
      if (restored == null) {
        // Cannot restore this source; signal the caller by returning the original schema.
        return schema;
      }
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
  private static <T> Expression unboundWithName(UnboundPredicate<T> pred, String name) {
    UnboundTerm<T> term = renameTerm(pred.term(), name);
    if (pred.literals() == null) {
      return Expressions.predicate(pred.op(), term);
    }
    List<T> values = pred.literals().stream().map(Literal::value).collect(Collectors.toList());
    return Expressions.predicate(pred.op(), term, values);
  }

  private static <T> Expression unboundFromBound(BoundPredicate<T> pred, String name) {
    if (pred.isUnaryPredicate()) {
      return Expressions.predicate(pred.op(), name);
    }
    if (pred.isLiteralPredicate()) {
      return Expressions.predicate(pred.op(), name, pred.asLiteralPredicate().literal().value());
    }
    if (pred.isSetPredicate()) {
      return Expressions.predicate(pred.op(), name, pred.asSetPredicate().literalSet());
    }
    // Fallback: preserve always-true rather than drop an unrecognised shape.
    return Expressions.alwaysTrue();
  }

  @SuppressWarnings("unchecked")
  private static <T> UnboundTerm<T> renameTerm(UnboundTerm<T> term, String name) {
    if (term instanceof UnboundTransform) {
      return Expressions.transform(name, ((UnboundTransform<?, T>) term).transform());
    }
    return (UnboundTerm<T>) Expressions.ref(name);
  }

  /**
   * Prefers an unpartitioned parseable spec; otherwise any parseable spec (typically the current
   * default after partition evolution). Iceberg REST clients resolve file {@code spec-id} against
   * their local {@code table.specs()}, so the target id must already exist there without {@code
   * unknown}.
   */
  @Nullable
  private static PartitionSpec findClientCompatibleSpec(Map<Integer, PartitionSpec> specsById) {
    PartitionSpec unpartitioned = null;
    PartitionSpec anyCompatible = null;
    int bestId = Integer.MIN_VALUE;
    for (PartitionSpec spec : specsById.values()) {
      if (partitionTypeHasUnknown(spec.partitionType())) {
        continue;
      }
      if (spec.isUnpartitioned()) {
        unpartitioned = spec;
      }
      if (spec.specId() >= bestId) {
        bestId = spec.specId();
        anyCompatible = spec;
      }
    }
    return unpartitioned != null ? unpartitioned : anyCompatible;
  }

  private static FileScanTask withResidual(
      FileScanTask task,
      Expression residual,
      @Nullable Schema residualSchema,
      boolean caseSensitive) {
    PartitionSpec fileSpec = task.spec();
    PartitionSpec evalSpec =
        residualSchema == null ? fileSpec : fileSpec.toUnbound().bind(residualSchema, true);
    ResidualEvaluator residuals =
        ResidualEvaluator.of(
            evalSpec, residual == null ? Expressions.alwaysTrue() : residual, caseSensitive);
    Schema schemaJson = residualSchema != null ? residualSchema : task.schema();
    return new BaseFileScanTask(
        task.file(),
        task.deletes().toArray(new DeleteFile[0]),
        SchemaParser.toJson(schemaJson),
        PartitionSpecParser.toJson(fileSpec),
        residuals);
  }

  private static FileScanTask withCompatibleSpec(
      FileScanTask task,
      PartitionSpec targetSpec,
      Expression residual,
      @Nullable Schema residualSchema,
      boolean caseSensitive) {
    // Rebuild under a client-parseable spec id so Iceberg clients can parse the task (and its
    // deletes) with their local table.specs() map, which still has UnknownType for dropped
    // partition sources on the historical spec id.
    DataFile rewritten = rewriteDataFile(task.file(), task.spec(), targetSpec);
    DeleteFile[] deletes = rewriteDeletes(task.deletes(), task.spec(), targetSpec);
    Schema evalSchema = residualSchema != null ? residualSchema : targetSpec.schema();
    PartitionSpec evalSpec = targetSpec.toUnbound().bind(evalSchema, true);
    ResidualEvaluator residuals =
        ResidualEvaluator.of(
            evalSpec, residual == null ? Expressions.alwaysTrue() : residual, caseSensitive);
    return new BaseFileScanTask(
        rewritten,
        deletes,
        SchemaParser.toJson(evalSchema),
        PartitionSpecParser.toJson(targetSpec),
        residuals);
  }

  private static DataFile rewriteDataFile(
      DataFile file, PartitionSpec fromSpec, PartitionSpec targetSpec) {
    if (targetSpec.isUnpartitioned()) {
      return DataFiles.builder(targetSpec).copy(file).build();
    }
    if (fromSpec.specId() == targetSpec.specId()) {
      return DataFiles.builder(targetSpec).copy(file).build();
    }
    DataFiles.Builder builder =
        DataFiles.builder(targetSpec)
            .withPath(file.location())
            .withFormat(file.format())
            .withFileSizeInBytes(file.fileSizeInBytes())
            .withRecordCount(file.recordCount());
    if (file.splitOffsets() != null) {
      builder.withSplitOffsets(file.splitOffsets());
    }
    if (file.sortOrderId() != null) {
      builder.withSortOrderId(file.sortOrderId());
    }
    if (file.keyMetadata() != null) {
      builder.withEncryptionKeyMetadata(file.keyMetadata());
    }
    StructLike projected = projectPartition(file.partition(), fromSpec, targetSpec);
    if (projected != null) {
      builder.withPartition(projected);
    }
    return builder.build();
  }

  private static DeleteFile[] rewriteDeletes(
      List<DeleteFile> deletes, PartitionSpec fromSpec, PartitionSpec targetSpec) {
    if (deletes == null || deletes.isEmpty()) {
      return new DeleteFile[0];
    }
    DeleteFile[] rewritten = new DeleteFile[deletes.size()];
    for (int i = 0; i < deletes.size(); i++) {
      rewritten[i] = rewriteDeleteFile(deletes.get(i), fromSpec, targetSpec);
    }
    return rewritten;
  }

  private static DeleteFile rewriteDeleteFile(
      DeleteFile delete, PartitionSpec fromSpec, PartitionSpec targetSpec) {
    if (targetSpec.isUnpartitioned() || fromSpec.specId() == targetSpec.specId()) {
      FileMetadata.Builder builder = FileMetadata.deleteFileBuilder(targetSpec).copy(delete);
      return builder.build();
    }
    FileMetadata.Builder builder = FileMetadata.deleteFileBuilder(targetSpec);
    switch (delete.content()) {
      case POSITION_DELETES:
        builder.ofPositionDeletes();
        break;
      case EQUALITY_DELETES:
        List<Integer> equalityFieldIds = delete.equalityFieldIds();
        int[] eqIds =
            equalityFieldIds == null
                ? new int[0]
                : equalityFieldIds.stream().mapToInt(Integer::intValue).toArray();
        builder.ofEqualityDeletes(eqIds);
        break;
      default:
        throw new IllegalArgumentException("Unsupported delete content: " + delete.content());
    }
    builder
        .withPath(delete.location())
        .withFormat(delete.format())
        .withFileSizeInBytes(delete.fileSizeInBytes())
        .withRecordCount(delete.recordCount());
    if (delete.referencedDataFile() != null) {
      builder.withReferencedDataFile(delete.referencedDataFile());
    }
    if (delete.contentOffset() != null) {
      builder.withContentOffset(delete.contentOffset());
    }
    if (delete.contentSizeInBytes() != null) {
      builder.withContentSizeInBytes(delete.contentSizeInBytes());
    }
    StructLike projected = projectPartition(delete.partition(), fromSpec, targetSpec);
    if (projected != null) {
      builder.withPartition(projected);
    }
    return builder.build();
  }

  @Nullable
  private static StructLike projectPartition(
      StructLike partition, PartitionSpec fromSpec, PartitionSpec toSpec) {
    if (toSpec.isUnpartitioned() || partition == null) {
      return null;
    }
    PartitionData out = new PartitionData(toSpec.partitionType());
    List<PartitionField> fromFields = fromSpec.fields();
    List<PartitionField> toFields = toSpec.fields();
    for (int toPos = 0; toPos < toFields.size(); toPos++) {
      PartitionField toField = toFields.get(toPos);
      int fromPos = -1;
      for (int i = 0; i < fromFields.size(); i++) {
        PartitionField fromField = fromFields.get(i);
        if (fromField.sourceId() == toField.sourceId()
            && fromField.transform().toString().equals(toField.transform().toString())) {
          fromPos = i;
          break;
        }
      }
      if (fromPos >= 0) {
        out.set(toPos, partition.get(fromPos, Object.class));
      }
    }
    return out;
  }
}
