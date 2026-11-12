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
package org.apache.gravitino.server.web.filter;

import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.ALTER_TABLE_AUTHORIZATION_EXPRESSION;
import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.CREATE_TABLE_AUTHORIZATION_EXPRESSION;
import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.REPLACE_TABLE_AUTHORIZATION_EXPRESSION;
import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.WRITE_TABLE_AUTHORIZATION_EXPRESSION;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.iceberg.service.IcebergRESTUtils;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.server.web.filter.BaseMetadataAuthorizationMethodInterceptor.AuthorizationHandler;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.rest.requests.UpdateTableRequest;

/** Authorizes creation, metadata alterations, and data writes before Iceberg executes a commit. */
public class UpdateTableAuthzHandler implements AuthorizationHandler {
  private static final Set<Class<? extends MetadataUpdate>> DATA_UPDATES =
      Set.of(
          MetadataUpdate.AddSnapshot.class,
          MetadataUpdate.RemoveSnapshots.class,
          MetadataUpdate.SetSnapshotRef.class,
          MetadataUpdate.RemoveSnapshotRef.class,
          MetadataUpdate.SetStatistics.class,
          MetadataUpdate.RemoveStatistics.class,
          MetadataUpdate.SetPartitionStatistics.class,
          MetadataUpdate.RemovePartitionStatistics.class);

  private static final Set<Class<? extends MetadataUpdate>> METADATA_UPDATES =
      Set.of(
          MetadataUpdate.AssignUUID.class,
          MetadataUpdate.UpgradeFormatVersion.class,
          MetadataUpdate.AddSchema.class,
          MetadataUpdate.SetCurrentSchema.class,
          MetadataUpdate.RemoveSchemas.class,
          MetadataUpdate.AddPartitionSpec.class,
          MetadataUpdate.SetDefaultPartitionSpec.class,
          MetadataUpdate.RemovePartitionSpecs.class,
          MetadataUpdate.AddSortOrder.class,
          MetadataUpdate.SetDefaultSortOrder.class,
          MetadataUpdate.SetProperties.class,
          MetadataUpdate.RemoveProperties.class,
          MetadataUpdate.SetLocation.class,
          MetadataUpdate.AddEncryptionKey.class,
          MetadataUpdate.RemoveEncryptionKey.class);

  private final boolean stagedCreate;
  private final String expression;

  /**
   * Creates a handler using the same staged-create predicate as the table hook dispatcher.
   *
   * @param request the table commit request
   */
  public UpdateTableAuthzHandler(UpdateTableRequest request) {
    IcebergRESTUtils.validateRequest(request, "UpdateTableRequest");
    stagedCreate =
        request.requirements().stream()
            .anyMatch(UpdateRequirement.AssertTableDoesNotExist.class::isInstance);
    if (stagedCreate) {
      expression = CREATE_TABLE_AUTHORIZATION_EXPRESSION;
      return;
    }
    boolean altersMetadata = false;
    boolean writesData = request.updates().isEmpty();
    for (MetadataUpdate update : request.updates()) {
      if (DATA_UPDATES.stream().anyMatch(type -> type.isInstance(update))) {
        writesData = true;
      } else if (METADATA_UPDATES.stream().anyMatch(type -> type.isInstance(update))) {
        altersMetadata = true;
      } else {
        // An unknown update may change either schema or data. Require both until classified.
        altersMetadata = true;
        writesData = true;
      }
    }
    expression =
        altersMetadata && writesData
            ? REPLACE_TABLE_AUTHORIZATION_EXPRESSION
            : altersMetadata
                ? ALTER_TABLE_AUTHORIZATION_EXPRESSION
                : WRITE_TABLE_AUTHORIZATION_EXPRESSION;
  }

  @Override
  public void process(Map<EntityType, NameIdentifier> nameIdentifierMap) {
    if (!new AuthorizationExpressionEvaluator(expression)
        .evaluate(
            nameIdentifierMap,
            new HashMap<>(),
            new AuthorizationRequestContext(),
            Optional.of(entityType().name()))) {
      throw new ForbiddenException(
          "Not authorized to commit table changes in '%s'", nameIdentifierMap.get(entityType()));
    }
  }

  @Override
  public boolean authorizationCompleted() {
    return true;
  }

  @Override
  public EntityType entityType() {
    return stagedCreate ? EntityType.SCHEMA : EntityType.TABLE;
  }
}
