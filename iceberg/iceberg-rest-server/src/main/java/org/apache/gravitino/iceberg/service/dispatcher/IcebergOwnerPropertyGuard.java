/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.iceberg.service.dispatcher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;

/**
 * Keeps the {@code owner} property of namespaces and tables under Gravitino's control. The Iceberg
 * REST catalog sets it to the creator, and an ownership transfer in Gravitino rewrites it; a
 * property update through the Iceberg REST catalog cannot change it.
 *
 * <p>Updates to the property are dropped rather than rejected: engines send it on their own, for
 * example Spark adds the current user as {@code owner} when it creates or replaces a table, and
 * rejecting it would fail those statements.
 */
final class IcebergOwnerPropertyGuard {

  private IcebergOwnerPropertyGuard() {}

  /**
   * Returns the request without changes to the owner property. A commit that creates the table sets
   * the property to the authenticated user instead, as a direct create does.
   *
   * @param request the table commit
   * @param user the authenticated user
   * @return the request to apply
   */
  static UpdateTableRequest protect(UpdateTableRequest request, String user) {
    if (AuthConstants.ANONYMOUS_USER.equals(user) || !touchesOwner(request)) {
      return request;
    }
    boolean creates =
        request.requirements().stream()
            .anyMatch(UpdateRequirement.AssertTableDoesNotExist.class::isInstance);
    List<MetadataUpdate> updates = new ArrayList<>();
    for (MetadataUpdate update : request.updates()) {
      if (update instanceof MetadataUpdate.SetProperties) {
        Map<String, String> properties =
            new HashMap<>(((MetadataUpdate.SetProperties) update).updated());
        if (creates) {
          properties.put(IcebergConstants.OWNER, user);
        } else {
          properties.remove(IcebergConstants.OWNER);
        }
        if (!properties.isEmpty()) {
          updates.add(new MetadataUpdate.SetProperties(properties));
        }
      } else if (update instanceof MetadataUpdate.RemoveProperties) {
        List<String> removed =
            ((MetadataUpdate.RemoveProperties) update)
                .removed().stream()
                    .filter(key -> !IcebergConstants.OWNER.equals(key))
                    .collect(Collectors.toList());
        if (!removed.isEmpty()) {
          updates.add(new MetadataUpdate.RemoveProperties(new HashSet<>(removed)));
        }
      } else {
        updates.add(update);
      }
    }
    return request.identifier() == null
        ? new UpdateTableRequest(request.requirements(), updates)
        : UpdateTableRequest.create(request.identifier(), request.requirements(), updates);
  }

  /**
   * Returns the request without changes to the owner property.
   *
   * @param request the namespace property update
   * @param user the authenticated user
   * @return the request to apply
   */
  static UpdateNamespacePropertiesRequest protect(
      UpdateNamespacePropertiesRequest request, String user) {
    if (AuthConstants.ANONYMOUS_USER.equals(user)
        || (!request.updates().containsKey(IcebergConstants.OWNER)
            && !request.removals().contains(IcebergConstants.OWNER))) {
      return request;
    }
    Map<String, String> updates = new HashMap<>(request.updates());
    updates.remove(IcebergConstants.OWNER);
    List<String> removals =
        request.removals().stream()
            .filter(key -> !IcebergConstants.OWNER.equals(key))
            .collect(Collectors.toList());
    return UpdateNamespacePropertiesRequest.builder()
        .updateAll(updates)
        .removeAll(removals)
        .build();
  }

  private static boolean touchesOwner(UpdateTableRequest request) {
    return request.updates().stream()
        .anyMatch(
            update ->
                (update instanceof MetadataUpdate.SetProperties
                        && ((MetadataUpdate.SetProperties) update)
                            .updated()
                            .containsKey(IcebergConstants.OWNER))
                    || (update instanceof MetadataUpdate.RemoveProperties
                        && ((MetadataUpdate.RemoveProperties) update)
                            .removed()
                            .contains(IcebergConstants.OWNER)));
  }
}
