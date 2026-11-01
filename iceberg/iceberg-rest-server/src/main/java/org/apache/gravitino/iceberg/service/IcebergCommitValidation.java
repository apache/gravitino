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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.MetadataUpdate;
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
  public static UpdateTableRequest withValidation(UpdateTableRequest request) {
    if (request == null || request.updates() == null || request.updates().isEmpty()) {
      return request;
    }
    List<UpdateRequirement> requirements =
        ImmutableList.<UpdateRequirement>builder()
            .addAll(request.requirements() == null ? ImmutableList.of() : request.requirements())
            .add(new IcebergCommitValidation(request.updates()))
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
