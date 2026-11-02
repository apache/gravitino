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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
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
  public static UpdateTableRequest withValidation(UpdateTableRequest request) {
    return withValidation(request, () -> null);
  }

  /**
   * Returns the request with the commit checks added to its requirements, reading manifest lists
   * through the given FileIO where a check needs them.
   *
   * @param request the table commit request
   * @param fileIO supplies the table's FileIO; only called when a check reads manifest lists
   * @return a request with the same updates and the checks as an additional requirement
   */
  public static UpdateTableRequest withValidation(
      UpdateTableRequest request, Supplier<FileIO> fileIO) {
    if (request == null || request.updates() == null || request.updates().isEmpty()) {
      return request;
    }
    List<UpdateRequirement> requirements =
        ImmutableList.<UpdateRequirement>builder()
            .addAll(request.requirements() == null ? ImmutableList.of() : request.requirements())
            .add(new IcebergCommitValidation(request.updates(), fileIO))
            .build();
    return new UpdateTableRequest(requirements, request.updates());
  }

  @Override
  public void validate(TableMetadata base) {
    if (base == null) {
      // A create commit: there is no existing metadata to check against.
      return;
    }
    validate(base, updates);
    checkRemovedSpecsAreUnused(base);
    checkSchemas(base);
    checkPartitionSpecs(base);
  }

  @VisibleForTesting
  static void validate(TableMetadata base, List<MetadataUpdate> updates) {
    boolean addsSchema = false;
    Set<Long> removedSnapshots = new HashSet<>();
    Set<String> changedRefs = new HashSet<>();
    for (MetadataUpdate update : updates) {
      if (update instanceof MetadataUpdate.AddSchema) {
        addsSchema = true;
      } else if (update instanceof MetadataUpdate.RemoveSnapshots) {
        removedSnapshots.addAll(((MetadataUpdate.RemoveSnapshots) update).snapshotIds());
      } else if (update instanceof MetadataUpdate.RemoveSnapshotRef) {
        changedRefs.add(((MetadataUpdate.RemoveSnapshotRef) update).name());
      } else if (update instanceof MetadataUpdate.SetSnapshotRef) {
        MetadataUpdate.SetSnapshotRef setRef = (MetadataUpdate.SetSnapshotRef) update;
        checkMainIsBranch(setRef);
        changedRefs.add(setRef.name());
      }
    }

    for (MetadataUpdate update : updates) {
      if (update instanceof MetadataUpdate.AddSnapshot) {
        checkSnapshot(base, ((MetadataUpdate.AddSnapshot) update).snapshot(), addsSchema);
      }
    }

    // Iceberg drops every ref that points at a removed snapshot. Removing a snapshot that a branch
    // or tag still references is only valid when the same commit also moves or removes that ref.
    for (Map.Entry<String, SnapshotRef> ref : base.refs().entrySet()) {
      if (removedSnapshots.contains(ref.getValue().snapshotId())
          && !changedRefs.contains(ref.getKey())) {
        throw new CommitFailedException(
            "Cannot remove snapshot %s: it is referenced by %s %s",
            ref.getValue().snapshotId(),
            ref.getValue().isBranch() ? "branch" : "tag",
            ref.getKey());
      }
    }
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
   * An added schema must keep the data already written readable: a field id that exists in the
   * current schema keeps its kind and may only change type by an allowed promotion, a field id not
   * in the current schema must be new (above last-column-id) or identical to the field that had the
   * id before, and a new required field needs a default. A replace (which removes main) starts from
   * no data, so only the id rule applies to it.
   */
  private void checkSchemas(TableMetadata base) {
    boolean replacesTable =
        updates.stream()
            .anyMatch(
                u ->
                    u instanceof MetadataUpdate.RemoveSnapshotRef
                        && SnapshotRef.MAIN_BRANCH.equals(
                            ((MetadataUpdate.RemoveSnapshotRef) u).name()));
    boolean hasData = !base.snapshots().isEmpty();
    Map<Integer, Types.NestedField> current = TypeUtil.indexById(base.schema().asStruct());
    for (MetadataUpdate update : updates) {
      if (!(update instanceof MetadataUpdate.AddSchema)) {
        continue;
      }
      Schema schema = ((MetadataUpdate.AddSchema) update).schema();
      for (Types.NestedField field : TypeUtil.indexById(schema.asStruct()).values()) {
        Types.NestedField existing = current.get(field.fieldId());
        if (existing == null) {
          checkNewField(base, field, hasData && !replacesTable);
        } else if (hasData && !replacesTable) {
          checkTypeChange(existing, field);
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
          "Invalid schema: required field %s has no initial default, so existing rows cannot be"
              + " read",
          field.name());
    }
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

  private static void checkTypeChange(Types.NestedField existing, Types.NestedField field) {
    Type from = existing.type();
    Type to = field.type();
    if (from.equals(to)) {
      return;
    }
    if (from.isPrimitiveType() && to.isPrimitiveType()) {
      if (!TypeUtil.isPromotionAllowed(from, to.asPrimitiveType())) {
        throw new ValidationException(
            "Invalid schema: field %s cannot change type from %s to %s", field.name(), from, to);
      }
    } else if (from.typeId() != to.typeId()) {
      throw new ValidationException(
          "Invalid schema: field %s cannot change from %s to %s", field.name(), from, to);
    }
  }

  /**
   * Partition field ids identify partition data across specs (v2+): a field id another spec uses
   * for a different field, or a new field id at or below last-partition-id, would mix up partition
   * values. A spec or sort order with a transform the table spec does not define cannot be the
   * default: writers are not allowed to commit data with it.
   */
  private void checkPartitionSpecs(TableMetadata base) {
    Schema schema = schemaForSpecs(base);
    boolean setsDefaultSpec = false;
    boolean setsDefaultOrder = false;
    List<PartitionSpec> added = new ArrayList<>();
    List<SortOrder> addedOrders = new ArrayList<>();
    for (MetadataUpdate update : updates) {
      if (update instanceof MetadataUpdate.AddPartitionSpec) {
        PartitionSpec spec = bindOrNull(((MetadataUpdate.AddPartitionSpec) update).spec(), schema);
        if (spec != null) {
          added.add(spec);
        }
      } else if (update instanceof MetadataUpdate.AddSortOrder) {
        try {
          addedOrders.add(((MetadataUpdate.AddSortOrder) update).sortOrder().bind(schema));
        } catch (RuntimeException e) {
          // Iceberg reports a sort order it cannot bind when it applies the update.
        }
      } else if (update instanceof MetadataUpdate.SetDefaultPartitionSpec) {
        setsDefaultSpec = true;
        int specId = ((MetadataUpdate.SetDefaultPartitionSpec) update).specId();
        PartitionSpec existing = base.specsById().get(specId);
        if (existing != null) {
          checkKnownTransforms(existing.fields(), "partition spec " + specId);
        }
      } else if (update instanceof MetadataUpdate.SetDefaultSortOrder) {
        setsDefaultOrder = true;
        int orderId = ((MetadataUpdate.SetDefaultSortOrder) update).sortOrderId();
        SortOrder existing = base.sortOrdersById().get(orderId);
        if (existing != null) {
          checkKnownSortTransforms(existing, "sort order " + orderId);
        }
      }
    }

    for (PartitionSpec spec : added) {
      if (base.formatVersion() >= 2) {
        checkPartitionFieldIds(base, spec);
      }
      if (setsDefaultSpec) {
        checkKnownTransforms(spec.fields(), "the added partition spec");
      }
    }
    if (setsDefaultOrder) {
      for (SortOrder order : addedOrders) {
        checkKnownSortTransforms(order, "the added sort order");
      }
    }
  }

  private static void checkPartitionFieldIds(TableMetadata base, PartitionSpec spec) {
    Map<Integer, PartitionField> existingFields = new HashMap<>();
    for (PartitionSpec existing : base.specs()) {
      for (PartitionField field : existing.fields()) {
        existingFields.putIfAbsent(field.fieldId(), field);
      }
    }
    int lastAssigned = Math.max(base.lastAssignedPartitionId(), PARTITION_DATA_ID_START - 1);
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
            field, base.lastAssignedPartitionId());
      }
    }
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

  private Schema schemaForSpecs(TableMetadata base) {
    Schema added = null;
    for (MetadataUpdate update : updates) {
      if (update instanceof MetadataUpdate.AddSchema) {
        added = ((MetadataUpdate.AddSchema) update).schema();
      } else if (update instanceof MetadataUpdate.SetCurrentSchema && added != null) {
        int schemaId = ((MetadataUpdate.SetCurrentSchema) update).schemaId();
        if (schemaId == -1 || schemaId == added.schemaId()) {
          return added;
        }
      }
    }
    return base.schema();
  }

  private static PartitionSpec bindOrNull(UnboundPartitionSpec spec, Schema schema) {
    try {
      return spec.bind(schema);
    } catch (RuntimeException e) {
      // Iceberg reports a spec it cannot bind when it applies the update.
      return null;
    }
  }

  private static void checkMainIsBranch(MetadataUpdate.SetSnapshotRef setRef) {
    if (SnapshotRef.MAIN_BRANCH.equals(setRef.name())
        && SnapshotRefType.TAG.name().equalsIgnoreCase(setRef.type())) {
      throw new ValidationException(
          "Cannot set %s to a tag: it must be a branch", SnapshotRef.MAIN_BRANCH);
    }
  }

  private static void checkSnapshot(TableMetadata base, Snapshot snapshot, boolean addsSchema) {
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
    if (schemaId != null && !addsSchema && !base.schemasById().containsKey(schemaId)) {
      throw new ValidationException(
          "Invalid snapshot %s: the table has no schema with id %s",
          snapshot.snapshotId(), schemaId);
    }
  }
}
