/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.iceberg.service;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.RetryableValidationException;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotRefType;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.UnboundPartitionSpec;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;

/**
 * Checks a table commit against the metadata it is applied to, before Iceberg applies it.
 *
 * <p>Iceberg applies some metadata updates without checking them, and the result is a table that
 * clients can no longer write or load. The checks run as an extra {@link UpdateRequirement}, so
 * Iceberg evaluates them against the exact metadata the commit is built on, again on every retry. A
 * check that depends on the table's current refs therefore cannot be bypassed by a concurrent
 * commit.
 */
public final class IcebergCommitValidation implements UpdateRequirement {

  private static final Set<String> SNAPSHOT_OPERATIONS =
      ImmutableSet.of(
          DataOperations.APPEND,
          DataOperations.REPLACE,
          DataOperations.OVERWRITE,
          DataOperations.DELETE);

  /** Transforms the table spec defines; anything else is an unknown transform. */
  private static final Pattern KNOWN_TRANSFORM =
      Pattern.compile("identity|year|month|day|hour|void|bucket\\[\\d+]|truncate\\[\\d+]");

  private static final int PARTITION_DATA_ID_START = 1000;

  private final List<MetadataUpdate> updates;
  private final Supplier<FileIO> fileIO;

  private IcebergCommitValidation(List<MetadataUpdate> updates, Supplier<FileIO> fileIO) {
    this.updates = updates;
    this.fileIO = fileIO;
  }

  /**
   * Returns the request with the commit checks added to its requirements. The request itself is not
   * changed.
   *
   * @param request the table commit request
   * @return a request with the same updates and the checks as an additional requirement
   */
  @Nullable
  public static UpdateTableRequest withValidation(@Nullable UpdateTableRequest request) {
    return withValidation(request, () -> null);
  }

  /**
   * Returns the request with commit checks that use the table's FileIO to read manifest lists.
   *
   * @param request the table commit request
   * @param fileIO supplies the table's FileIO only when a check reads manifest lists
   * @return a request with validation, or the original request when it has no updates
   */
  @Nullable
  public static UpdateTableRequest withValidation(
      @Nullable UpdateTableRequest request, Supplier<FileIO> fileIO) {
    if (request == null || request.updates() == null || request.updates().isEmpty()) {
      return request;
    }
    ImmutableList.Builder<UpdateRequirement> requirements = ImmutableList.builder();
    boolean create = false;
    for (UpdateRequirement requirement : request.requirements()) {
      if (requirement instanceof UpdateRequirement.AssertTableDoesNotExist) {
        // CatalogHandlers accepts only AssertTableDoesNotExist requirements for a staged create.
        requirements.add(new CreateValidation(request.updates(), fileIO));
        create = true;
      } else {
        requirements.add(requirement);
      }
    }
    if (!create) {
      requirements.add(new IcebergCommitValidation(request.updates(), fileIO));
    }
    return new UpdateTableRequest(requirements.build(), request.updates());
  }

  /**
   * Gives partition fields that an add-spec sends without a {@code field-id} the id the table
   * assigns: the id of the same field (source and transform) in an earlier spec, or the next id
   * above {@code last-partition-id}. The REST spec makes {@code field-id} optional, but Iceberg's
   * metadata builder numbers such fields from 1000 again, so they would take the ids of existing
   * partition fields.
   *
   * @param request the table commit request
   * @param current supplies the table's current metadata, or null if it cannot be loaded; called
   *     only when a field needs an id
   * @return the request with every added partition field carrying an id
   */
  public static UpdateTableRequest assignPartitionFieldIds(
      UpdateTableRequest request, Supplier<TableMetadata> current) {
    if (request == null
        || request.updates() == null
        || request.requirements().stream()
            .anyMatch(UpdateRequirement.AssertTableDoesNotExist.class::isInstance)) {
      // A create starts from no partition fields, so numbering from 1000 is right.
      return request;
    }
    boolean missingIds = false;
    for (MetadataUpdate update : request.updates()) {
      if (update instanceof MetadataUpdate.AddPartitionSpec) {
        for (Object field : fields(((MetadataUpdate.AddPartitionSpec) update).spec())) {
          missingIds |= partitionId(field) == null;
        }
      }
    }
    if (!missingIds) {
      return request;
    }
    TableMetadata base = current.get();
    if (base == null || base.formatVersion() < 2) {
      return request;
    }

    Map<String, Integer> knownIds = new HashMap<>();
    for (PartitionSpec spec : base.specs()) {
      for (PartitionField field : spec.fields()) {
        knownIds.putIfAbsent(
            partitionFieldKey(field.sourceId(), field.transform().toString()), field.fieldId());
      }
    }
    int lastAssigned = Math.max(base.lastAssignedPartitionId(), PARTITION_DATA_ID_START - 1);
    for (MetadataUpdate update : request.updates()) {
      if (update instanceof MetadataUpdate.AddPartitionSpec) {
        for (Object field : fields(((MetadataUpdate.AddPartitionSpec) update).spec())) {
          Integer id = partitionId(field);
          if (id != null) {
            knownIds.putIfAbsent(partitionFieldKey(field), id);
            lastAssigned = Math.max(lastAssigned, id);
          }
        }
      }
    }

    List<MetadataUpdate> updates = new ArrayList<>();
    for (MetadataUpdate update : request.updates()) {
      if (!(update instanceof MetadataUpdate.AddPartitionSpec)) {
        updates.add(update);
        continue;
      }
      UnboundPartitionSpec spec = ((MetadataUpdate.AddPartitionSpec) update).spec();
      ObjectNode specNode = JsonUtil.mapper().createObjectNode();
      specNode.put("spec-id", spec.specId());
      ArrayNode fields = specNode.putArray("fields");
      for (Object field : fields(spec)) {
        Integer id = partitionId(field);
        if (id == null) {
          String key = partitionFieldKey(field);
          id = knownIds.get(key);
          if (id == null) {
            id = ++lastAssigned;
            knownIds.put(key, id);
          }
        }
        fields
            .addObject()
            .put("name", (String) fieldValue(field, "name"))
            .put("transform", (String) fieldValue(field, "transformAsString"))
            .put("source-id", (Integer) fieldValue(field, "sourceId"))
            .put("field-id", id);
      }
      updates.add(new MetadataUpdate.AddPartitionSpec(PartitionSpecParser.fromJson(specNode)));
    }
    return request.identifier() == null
        ? new UpdateTableRequest(request.requirements(), updates)
        : UpdateTableRequest.create(request.identifier(), request.requirements(), updates);
  }

  private static List<?> fields(UnboundPartitionSpec spec) {
    return spec.fields();
  }

  @Nullable
  private static Integer partitionId(Object unboundField) {
    return (Integer) fieldValue(unboundField, "partitionId");
  }

  private static String partitionFieldKey(Object unboundField) {
    return partitionFieldKey(
        (Integer) fieldValue(unboundField, "sourceId"),
        (String) fieldValue(unboundField, "transformAsString"));
  }

  /**
   * Reads an accessor of {@code UnboundPartitionSpec.UnboundPartitionField}. Its accessors are
   * public but the class is package-private in Iceberg 1.11, and no public API exposes whether a
   * field was sent without a {@code field-id}.
   */
  private static Object fieldValue(Object unboundField, String accessor) {
    try {
      Method method = unboundField.getClass().getMethod(accessor);
      method.setAccessible(true);
      return method.invoke(unboundField);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Cannot read partition field " + accessor, e);
    }
  }

  private static String partitionFieldKey(int sourceId, String transform) {
    return sourceId + ":" + transform;
  }

  @Override
  public void validate(@Nullable TableMetadata base) {
    TableMetadata updated = validate(base, updates);
    checkRemovedSpecsAreUnused(updated);
    if (base != null) {
      checkSchemas(base, updated);
    }
    checkPartitionSpecs(base, updated);
  }

  @VisibleForTesting
  static TableMetadata validate(@Nullable TableMetadata base, List<MetadataUpdate> updates) {
    Set<Long> removedSnapshots = new HashSet<>();
    Map<String, Long> remainingRefs = new HashMap<>();
    if (base != null) {
      base.refs().forEach((name, ref) -> remainingRefs.put(name, ref.snapshotId()));
    }
    for (MetadataUpdate update : updates) {
      if (update instanceof MetadataUpdate.RemoveSnapshots) {
        removedSnapshots.addAll(((MetadataUpdate.RemoveSnapshots) update).snapshotIds());
      } else if (update instanceof MetadataUpdate.RemoveSnapshotRef) {
        remainingRefs.remove(((MetadataUpdate.RemoveSnapshotRef) update).name());
      } else if (update instanceof MetadataUpdate.SetSnapshotRef) {
        MetadataUpdate.SetSnapshotRef setRef = (MetadataUpdate.SetSnapshotRef) update;
        checkMainIsBranch(setRef);
        remainingRefs.put(setRef.name(), setRef.snapshotId());
      }
    }

    // Iceberg drops every ref that points at a removed snapshot. Removing a snapshot that a branch
    // or tag still references is only valid when the same commit also moves or removes that ref.
    for (Map.Entry<String, Long> ref : remainingRefs.entrySet()) {
      if (removedSnapshots.contains(ref.getValue())) {
        throw new CommitFailedException(
            "Cannot remove snapshot %s: it is referenced by ref %s", ref.getValue(), ref.getKey());
      }
    }

    // Apply only in memory. Iceberg assigns schema ids while applying AddSchema, so checking the
    // resulting metadata avoids accepting an arbitrary id just because the request adds a schema.
    TableMetadata.Builder builder;
    if (base == null) {
      builder =
          updates.stream()
              .filter(MetadataUpdate.UpgradeFormatVersion.class::isInstance)
              .map(MetadataUpdate.UpgradeFormatVersion.class::cast)
              .map(MetadataUpdate.UpgradeFormatVersion::formatVersion)
              .findFirst()
              .map(TableMetadata::buildFromEmpty)
              .orElseGet(TableMetadata::buildFromEmpty);
    } else {
      builder = TableMetadata.buildFrom(base);
    }
    try {
      updates.forEach(update -> update.applyTo(builder));
    } catch (RetryableValidationException e) {
      // Preserve CatalogHandlers' conflict response for stale sequence numbers and row ids.
      throw new CommitFailedException(e, "Validation failed, please retry: %s", e.getMessage());
    }
    TableMetadata updated = builder.build();
    for (MetadataUpdate update : updates) {
      if (update instanceof MetadataUpdate.AddSnapshot) {
        checkSnapshot(updated, ((MetadataUpdate.AddSnapshot) update).snapshot());
      }
    }
    return updated;
  }

  /**
   * A partition spec can only be removed when no manifest of any snapshot was written with it:
   * readers look up every manifest's spec in the table metadata.
   */
  private void checkRemovedSpecsAreUnused(TableMetadata base) {
    Set<Integer> removed = new HashSet<>();
    for (MetadataUpdate update : updates) {
      if (update instanceof MetadataUpdate.RemovePartitionSpecs) {
        removed.addAll(((MetadataUpdate.RemovePartitionSpecs) update).specIds());
      }
    }
    if (removed.isEmpty() || base.snapshots().isEmpty()) {
      return;
    }
    FileIO io = fileIO.get();
    if (io == null) {
      throw new ValidationException(
          "Cannot remove partition specs %s: their use by existing manifests cannot be checked",
          removed);
    }
    for (Snapshot snapshot : base.snapshots()) {
      for (ManifestFile manifest : snapshot.allManifests(io)) {
        if (removed.contains(manifest.partitionSpecId())) {
          throw new ValidationException(
              "Cannot remove partition spec %s: snapshot %s has manifests written with it",
              manifest.partitionSpecId(), snapshot.snapshotId());
        }
      }
    }
  }

  /**
   * An added schema must follow the Iceberg Table Spec schema-evolution rules
   * (https://iceberg.apache.org/spec/#schema-evolution): a field id that exists in the current
   * schema keeps its kind, may only change type by an allowed promotion, and cannot go from
   * optional to required; a field id not in the current schema must be new (above last-column-id)
   * or identical to the field that had the id before; and a new required field needs a default. The
   * same rules are what Iceberg's {@code UpdateSchema} / {@code SchemaUpdate} enforce, and what the
   * Gravitino API path applies.
   *
   * <p>Per that Table Spec section (and {@code SchemaUpdate}), these rules do <em>not</em> depend
   * on whether the table has snapshots: incompatible evolution is rejected on empty tables unless
   * {@code allowIncompatibleChanges()} is set. The Iceberg REST commit API likewise defines no
   * {@code hasData} exemption. Dropping IRC's old {@code hasData} gate is therefore the
   * Table-Spec-compatible behavior; empty-table clients that previously got 200 for illegal
   * evolutions may now get 4xx.
   *
   * <p>A replace that removes {@code main} is not schema evolution under the Table Spec: it starts
   * from no retained data, so only the field-id reuse rule applies and create-then-replace / staged
   * create still succeed.
   */
  private void checkSchemas(TableMetadata base, TableMetadata updated) {
    boolean replacesTable =
        updates.stream()
                .anyMatch(
                    u ->
                        u instanceof MetadataUpdate.RemoveSnapshotRef
                            && SnapshotRef.MAIN_BRANCH.equals(
                                ((MetadataUpdate.RemoveSnapshotRef) u).name()))
            && (updated.currentSnapshot() == null
                || base.snapshot(updated.currentSnapshot().snapshotId()) == null);
    Map<Integer, Types.NestedField> current = TypeUtil.indexById(base.schema().asStruct());
    for (MetadataUpdate update : updates) {
      if (!(update instanceof MetadataUpdate.AddSchema)) {
        continue;
      }
      Schema schema = ((MetadataUpdate.AddSchema) update).schema();
      for (Types.NestedField field : TypeUtil.indexById(schema.asStruct()).values()) {
        Types.NestedField existing = current.get(field.fieldId());
        // Table Spec schema evolution (no hasData exemption); same as UpdateSchema / Gravitino API.
        if (existing == null) {
          checkNewField(base, field, !replacesTable);
        } else if (!replacesTable) {
          checkTypeChange(existing, field, updated.formatVersion());
          checkNullabilityChange(existing, field);
        }
      }
    }
  }

  private static void checkNewField(
      TableMetadata base, Types.NestedField field, boolean checkRequired) {
    if (field.fieldId() <= base.lastColumnId()) {
      Types.NestedField previous = previousField(base, field.fieldId());
      boolean sameField =
          previous != null
              && previous.name().equals(field.name())
              && previous.type().equals(field.type());
      if (!sameField) {
        throw new ValidationException(
            "Invalid schema: field %s reuses id %s, which the table assigned before (last column"
                + " id %s); new fields need new ids",
            field.name(), field.fieldId(), base.lastColumnId());
      }
    }
    if (checkRequired && field.isRequired() && field.initialDefault() == null) {
      throw new ValidationException(
          "Invalid schema: required field %s has no initial default", field.name());
    }
  }

  /**
   * The v3 table spec also allows date to timestamp and date to timestamp_ns (without time zone),
   * which Iceberg's {@link TypeUtil#isPromotionAllowed} does not implement.
   */
  private static boolean isV3DatePromotion(Type from, Type to, int formatVersion) {
    return formatVersion >= 3
        && from.typeId() == Type.TypeID.DATE
        && (Types.TimestampType.withoutZone().equals(to)
            || Types.TimestampNanoType.withoutZone().equals(to));
  }

  private static Types.NestedField previousField(TableMetadata base, int fieldId) {
    for (Schema schema : base.schemas()) {
      Types.NestedField field = schema.findField(fieldId);
      if (field != null) {
        return field;
      }
    }
    return null;
  }

  private static void checkTypeChange(
      Types.NestedField existing, Types.NestedField field, int formatVersion) {
    Type from = existing.type();
    Type to = field.type();
    if (from.equals(to)) {
      return;
    }
    if (from.isPrimitiveType() && to.isPrimitiveType()) {
      if (!TypeUtil.isPromotionAllowed(from, to.asPrimitiveType())
          && !isV3DatePromotion(from, to, formatVersion)) {
        throw new ValidationException(
            "Invalid schema: field %s cannot change type from %s to %s", field.name(), from, to);
      }
    } else if (from.typeId() != to.typeId()) {
      throw new ValidationException(
          "Invalid schema: field %s cannot change from %s to %s", field.name(), from, to);
    }
  }

  /**
   * An optional field cannot become required: the same rule as Iceberg's {@code
   * UpdateSchema#requireColumn}, which refuses the change unless incompatible changes are
   * explicitly allowed.
   */
  private static void checkNullabilityChange(Types.NestedField existing, Types.NestedField field) {
    if (existing.isOptional() && field.isRequired()) {
      throw new ValidationException(
          "Invalid schema: field %s cannot change from optional to required", field.name());
    }
  }

  /**
   * Partition field ids identify partition data across specs (v2+): a field id another spec uses
   * for a different field, or a new field id at or below last-partition-id, would mix up partition
   * values. A spec or sort order with a transform the table spec does not define cannot be the
   * default: writers are not allowed to commit data with it.
   */
  private void checkPartitionSpecs(@Nullable TableMetadata base, TableMetadata updated) {
    boolean setsDefaultSpec = false;
    boolean setsDefaultOrder = false;
    for (MetadataUpdate update : updates) {
      if (update instanceof MetadataUpdate.SetDefaultPartitionSpec) {
        setsDefaultSpec = true;
      } else if (update instanceof MetadataUpdate.SetDefaultSortOrder) {
        setsDefaultOrder = true;
      }
    }

    // Check the actual defaults, not every spec/order added by the request. An unknown transform
    // is allowed in a retained, non-default spec/order.
    if (setsDefaultSpec || base == null) {
      checkKnownTransforms(updated.spec().fields(), "partition spec " + updated.defaultSpecId());
    }
    if (setsDefaultOrder || base == null) {
      checkKnownSortTransforms(updated.sortOrder(), "sort order " + updated.defaultSortOrderId());
    }

    if (updated.formatVersion() < 2) {
      return;
    }
    Map<Integer, PartitionField> existingFields = new HashMap<>();
    if (base != null) {
      for (PartitionSpec existing : base.specs()) {
        for (PartitionField field : existing.fields()) {
          existingFields.putIfAbsent(field.fieldId(), field);
        }
      }
    }
    int lastAssigned =
        base == null
            ? PARTITION_DATA_ID_START - 1
            : Math.max(base.lastAssignedPartitionId(), PARTITION_DATA_ID_START - 1);
    for (PartitionSpec spec : updated.specs()) {
      if (base == null || !base.specsById().containsKey(spec.specId())) {
        lastAssigned = checkPartitionFieldIds(existingFields, lastAssigned, spec);
      }
    }
  }

  private static int checkPartitionFieldIds(
      Map<Integer, PartitionField> existingFields, int lastAssigned, PartitionSpec spec) {
    int highestId = lastAssigned;
    for (PartitionField field : spec.fields()) {
      PartitionField existing = existingFields.get(field.fieldId());
      if (existing != null) {
        if (existing.sourceId() != field.sourceId()
            || !existing.transform().toString().equals(field.transform().toString())) {
          throw new ValidationException(
              "Invalid partition spec: field id %s is already used for %s, not %s",
              field.fieldId(), existing, field);
        }
      } else if (field.fieldId() <= lastAssigned) {
        throw new ValidationException(
            "Invalid partition spec: new field %s needs an id above last-partition-id %s",
            field, lastAssigned);
      }
      existingFields.putIfAbsent(field.fieldId(), field);
      highestId = Math.max(highestId, field.fieldId());
    }
    return highestId;
  }

  private static void checkKnownTransforms(List<PartitionField> fields, String what) {
    for (PartitionField field : fields) {
      checkKnownTransform(field.transform().toString(), field.name(), what);
    }
  }

  private static void checkKnownSortTransforms(SortOrder order, String what) {
    for (SortField field : order.fields()) {
      checkKnownTransform(field.transform().toString(), "source " + field.sourceId(), what);
    }
  }

  private static void checkKnownTransform(String transform, String field, String what) {
    if (!KNOWN_TRANSFORM.matcher(transform).matches()) {
      throw new ValidationException(
          "Cannot make %s the default: field %s uses transform %s, which the table spec does not"
              + " define, and writers cannot commit data with it",
          what, field, transform);
    }
  }

  private static void checkMainIsBranch(MetadataUpdate.SetSnapshotRef setRef) {
    if (SnapshotRef.MAIN_BRANCH.equals(setRef.name())
        && SnapshotRefType.TAG.name().equalsIgnoreCase(setRef.type())) {
      throw new ValidationException(
          "Cannot set %s to a tag: it must be a branch", SnapshotRef.MAIN_BRANCH);
    }
  }

  private static void checkSnapshot(TableMetadata base, Snapshot snapshot) {
    // The summary and its operation are required from format version 2 on.
    String operation = snapshot.operation();
    if (operation == null) {
      if (base.formatVersion() >= 2) {
        throw new ValidationException(
            "Invalid snapshot %s: a summary with an operation is required", snapshot.snapshotId());
      }
    } else if (!SNAPSHOT_OPERATIONS.contains(operation)) {
      throw new ValidationException(
          "Invalid snapshot %s: unknown operation '%s', must be one of %s",
          snapshot.snapshotId(), operation, SNAPSHOT_OPERATIONS);
    }
    Integer schemaId = snapshot.schemaId();
    if (schemaId != null && !base.schemasById().containsKey(schemaId)) {
      throw new ValidationException(
          "Invalid snapshot %s: the table has no schema with id %s",
          snapshot.snapshotId(), schemaId);
    }
  }

  private static final class CreateValidation extends UpdateRequirement.AssertTableDoesNotExist {
    private final IcebergCommitValidation validation;

    private CreateValidation(List<MetadataUpdate> updates, Supplier<FileIO> fileIO) {
      this.validation = new IcebergCommitValidation(updates, fileIO);
    }

    @Override
    public void validate(@Nullable TableMetadata base) {
      super.validate(base);
      validation.validate(base);
    }
  }
}
