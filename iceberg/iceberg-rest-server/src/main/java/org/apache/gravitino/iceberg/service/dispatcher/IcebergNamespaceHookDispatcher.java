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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.iceberg.common.utils.IcebergIdentifierUtils;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.lock.LockType;
import org.apache.gravitino.lock.TreeLockUtils;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;

/**
 * {@code IcebergSchemaHookDispatcher} is a decorator for {@link
 * IcebergNamespaceOperationDispatcher} that not only delegates namespace operations to the
 * underlying dispatcher but also executes some hook operations before or after the underlying
 * operations.
 */
public class IcebergNamespaceHookDispatcher implements IcebergNamespaceOperationDispatcher {

  private final IcebergNamespaceOperationDispatcher dispatcher;
  private final String metalake;

  public IcebergNamespaceHookDispatcher(IcebergNamespaceOperationDispatcher dispatcher) {
    this.dispatcher = dispatcher;
    this.metalake = IcebergRESTServerContext.getInstance().metalakeName();
  }

  @Override
  public CreateNamespaceResponse createNamespace(
      IcebergRequestContext context, CreateNamespaceRequest createRequest) {
    String catalogName = context.catalogName();
    Namespace leaf = createRequest.namespace();
    // Lock the top-level branch root rather than the whole catalog: any race on shared ancestor
    // ownership has to share leaf.level(0), so serializing per top-level branch is sufficient
    // and lets disjoint branches (e.g. A:... and X:...) create in parallel.
    return TreeLockUtils.doWithTreeLock(
        NameIdentifier.of(metalake, catalogName, leaf.level(0)),
        LockType.WRITE,
        () -> {
          // Pre-probe so we only claim ownership of truly-new ancestors, never overwriting
          // an existing parent's owner.
          List<Namespace> missingAncestors = getMissingAncestors(context, leaf);
          CreateNamespaceResponse response = dispatcher.createNamespace(context, createRequest);

          // SchemaMetaService.insertSchema splits the leaf's logical name and auto-creates a
          // Gravitino entity row for each ancestor, so a single leaf import covers the branch.
          // Failures propagate intentionally: swallowing would leave a namespace in Iceberg
          // that Gravitino doesn't know about.
          importSchema(catalogName, leaf);

          List<Namespace> newlyOwned = new ArrayList<>(missingAncestors);
          newlyOwned.add(leaf);
          IcebergOwnershipUtils.setSchemaOwners(
              metalake,
              catalogName,
              newlyOwned,
              context.userName(),
              GravitinoEnv.getInstance().ownerDispatcher());

          return response;
        });
  }

  /**
   * Returns all ancestor namespaces of {@code namespace} that do not currently exist in the
   * catalog. Uses {@link HierarchicalSchemaUtil#getAncestorNames} (the same utility as the
   * Gravitino REST API side) so the prefix-enumeration algorithm is shared.
   *
   * <p>For example, if {@code namespace} is {@code Namespace.of("A","B","C")} and only {@code
   * Namespace.of("A")} already exists, this returns {@code [Namespace.of("A","B")]}.
   */
  private List<Namespace> getMissingAncestors(IcebergRequestContext context, Namespace namespace) {
    String separator = HierarchicalSchemaUtil.schemaSeparator();
    String namespaceName = String.join(separator, namespace.levels());
    List<String> ancestorNames = HierarchicalSchemaUtil.getAncestorNames(namespaceName, separator);
    // Iterate from innermost ancestor outward: in the hierarchical schema model the existence
    // of an inner ancestor implies the existence of all its outer ancestors, so we can stop
    // probing once we hit one that exists.
    List<Namespace> missing = new ArrayList<>();
    for (int i = ancestorNames.size() - 1; i >= 0; i--) {
      Namespace ancestor = Namespace.of(ancestorNames.get(i).split(Pattern.quote(separator)));
      if (dispatcher.namespaceExists(context, ancestor)) {
        break;
      }
      missing.add(ancestor);
    }
    // Reverse so the result is outermost-to-innermost (the order callers consume).
    Collections.reverse(missing);
    return missing;
  }

  @Override
  public UpdateNamespacePropertiesResponse updateNamespace(
      IcebergRequestContext context,
      Namespace namespace,
      UpdateNamespacePropertiesRequest updateRequest) {
    return dispatcher.updateNamespace(context, namespace, updateRequest);
  }

  @Override
  public void dropNamespace(IcebergRequestContext context, Namespace namespace) {
    dispatcher.dropNamespace(context, namespace);

    // Clean up the schema from Gravitino's entity store after successful drop
    EntityStore store = GravitinoEnv.getInstance().entityStore();
    try {
      if (store != null) {
        // Delete the entity for the dropped namespace (schema).
        store.delete(
            IcebergIdentifierUtils.toGravitinoSchemaIdentifier(
                metalake,
                context.catalogName(),
                namespace,
                HierarchicalSchemaUtil.schemaSeparator()),
            Entity.EntityType.SCHEMA);
      }
    } catch (NoSuchEntityException ignore) {
      // Ignore if the schema entity does not exist.
    } catch (IOException ioe) {
      throw new RuntimeException("io exception when deleting schema entity", ioe);
    }
  }

  @Override
  public GetNamespaceResponse loadNamespace(IcebergRequestContext context, Namespace namespace) {
    return dispatcher.loadNamespace(context, namespace);
  }

  @Override
  public ListNamespacesResponse listNamespaces(
      IcebergRequestContext context, Namespace parentNamespace) {
    return dispatcher.listNamespaces(context, parentNamespace);
  }

  @Override
  public boolean namespaceExists(IcebergRequestContext context, Namespace namespace) {
    return dispatcher.namespaceExists(context, namespace);
  }

  @Override
  public LoadTableResponse registerTable(
      IcebergRequestContext context,
      Namespace namespace,
      RegisterTableRequest registerTableRequest) {
    LoadTableResponse response = dispatcher.registerTable(context, namespace, registerTableRequest);

    // Import is intentionally NOT wrapped in try-catch: if it fails the table exists in Iceberg
    // but not in Gravitino, and silently swallowing that would mislead callers into thinking the
    // entity is registered. Surface the failure so the caller can react.
    importTable(context.catalogName(), namespace, registerTableRequest.name());

    IcebergOwnershipUtils.setTableOwner(
        metalake,
        context.catalogName(),
        namespace,
        registerTableRequest.name(),
        context.userName(),
        GravitinoEnv.getInstance().ownerDispatcher());

    return response;
  }

  private void importTable(String catalogName, Namespace namespace, String tableName) {
    TableDispatcher tableDispatcher = GravitinoEnv.getInstance().tableDispatcher();
    if (tableDispatcher != null) {
      tableDispatcher.loadTable(
          IcebergIdentifierUtils.toGravitinoTableIdentifier(
              metalake,
              catalogName,
              TableIdentifier.of(namespace, tableName),
              HierarchicalSchemaUtil.schemaSeparator()));
    }
  }

  private void importSchema(String catalogName, Namespace namespace) {
    SchemaDispatcher schemaDispatcher = GravitinoEnv.getInstance().schemaDispatcher();
    if (schemaDispatcher != null) {
      schemaDispatcher.loadSchema(
          IcebergIdentifierUtils.toGravitinoSchemaIdentifier(
              metalake, catalogName, namespace, HierarchicalSchemaUtil.schemaSeparator()));
    }
  }
}
