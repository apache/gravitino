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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.RetryableValidationException;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotRefType;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.rest.requests.UpdateTableRequest;

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

  private final List<MetadataUpdate> updates;

  private IcebergCommitValidation(List<MetadataUpdate> updates) {
    this.updates = updates;
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
    if (request == null || request.updates() == null || request.updates().isEmpty()) {
      return request;
    }
    ImmutableList.Builder<UpdateRequirement> requirements = ImmutableList.builder();
    boolean create = false;
    for (UpdateRequirement requirement : request.requirements()) {
      if (requirement instanceof UpdateRequirement.AssertTableDoesNotExist) {
        // CatalogHandlers accepts only AssertTableDoesNotExist requirements for a staged create.
        requirements.add(new CreateValidation(request.updates()));
        create = true;
      } else {
        requirements.add(requirement);
      }
    }
    if (!create) {
      requirements.add(new IcebergCommitValidation(request.updates()));
    }
    return new UpdateTableRequest(requirements.build(), request.updates());
  }

  @Override
  public void validate(@Nullable TableMetadata base) {
    validate(base, updates);
  }

  @VisibleForTesting
  static void validate(@Nullable TableMetadata base, List<MetadataUpdate> updates) {
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
    private final List<MetadataUpdate> updates;

    private CreateValidation(List<MetadataUpdate> updates) {
      this.updates = updates;
    }

    @Override
    public void validate(@Nullable TableMetadata base) {
      super.validate(base);
      IcebergCommitValidation.validate(base, updates);
    }
  }
}
