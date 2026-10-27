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
package org.apache.gravitino.iceberg.service.restriction;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.UserGroup;
import org.apache.gravitino.UserPrincipal;
import org.apache.gravitino.authorization.AccessControlDispatcher;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.iceberg.common.utils.IcebergIdentifierUtils;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.policy.ColumnMaskContent;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyDispatcher;
import org.apache.gravitino.policy.RowFilterContent;
import org.apache.gravitino.policy.rego.RestrictedRegoExpressionParserFacade;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.ColumnMask;
import org.apache.gravitino.policy.rego.RestrictedRegoProgram.RowFilter;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.restrictions.ReadRestrictions;
import org.apache.iceberg.rest.restrictions.ReadRestrictionsParser;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Loads tag-selected Gravitino policies into experimental Iceberg read restrictions. */
public final class ExperimentalIcebergReadRestrictionLoader
    implements IcebergReadRestrictionLoader {
  private static final Logger LOG =
      LoggerFactory.getLogger(ExperimentalIcebergReadRestrictionLoader.class);

  /** Creates the service-loaded read-restriction loader. */
  public ExperimentalIcebergReadRestrictionLoader() {}

  @Override
  public boolean enabled() {
    return GravitinoEnv.getInstance().internalPolicyDispatcher() != null;
  }

  @Override
  public Result load(
      IcebergRequestContext context, TableIdentifier tableIdentifier, LoadTableResponse response) {
    GravitinoEnv environment = GravitinoEnv.getInstance();
    PolicyDispatcher policyDispatcher = environment.internalPolicyDispatcher();
    AccessControlDispatcher accessControlDispatcher = environment.internalAccessControlDispatcher();
    if (policyDispatcher == null) {
      return Result.of(response, Optional.empty());
    }
    if (accessControlDispatcher == null) {
      throw new IllegalStateException(
          "Iceberg read restrictions require authorization; set gravitino.authorization.enable=true");
    }

    String metalake = IcebergRESTServerContext.getInstance().metalakeName();
    NameIdentifier tableName =
        IcebergIdentifierUtils.toGravitinoTableIdentifier(
            metalake,
            context.catalogName(),
            tableIdentifier,
            HierarchicalSchemaUtil.schemaSeparator());
    NameIdentifierUtil.checkMetadataObjectName(
        NameIdentifierUtil.ofMetalake(metalake), Entity.EntityType.METALAKE);
    NameIdentifierUtil.checkMetadataObjectName(
        NameIdentifierUtil.ofCatalog(metalake, context.catalogName()), Entity.EntityType.CATALOG);
    NameIdentifierUtil.checkMetadataObjectName(
        NameIdentifierUtil.ofSchema(
            metalake, context.catalogName(), tableName.namespace().level(2)),
        Entity.EntityType.SCHEMA);
    Schema schema = response.tableMetadata().schema();
    PolicyEntity[] rowPolicies =
        policyDispatcher.listPolicyInfosForMetadataObject(
            metalake, NameIdentifierUtil.toMetadataObject(tableName, Entity.EntityType.TABLE));
    Map<Integer, PolicyEntity[]> maskPolicies = new LinkedHashMap<>();
    for (Types.NestedField field : schema.columns()) {
      NameIdentifier columnName =
          NameIdentifierUtil.ofColumn(
              metalake,
              context.catalogName(),
              tableName.namespace().level(2),
              tableName.name(),
              field.name());
      MetadataObject column =
          NameIdentifierUtil.toMetadataObject(columnName, Entity.EntityType.COLUMN);
      maskPolicies.put(
          field.fieldId(), policyDispatcher.listPolicyInfosForMetadataObject(metalake, column));
    }

    IdentityBindingContext bindingContext =
        new IdentityBindingContext(context.userName(), metalake, accessControlDispatcher);
    ReadRestrictions restrictions =
        resolveForLoad(rowPolicies, maskPolicies, schema, bindingContext);
    if (restrictions.isEmpty()) {
      return Result.of(response, Optional.empty());
    }

    LoadTableResponse.Builder builder =
        copyResponseBuilder(response, response.tableMetadata()).withReadRestrictions(restrictions);
    String signature = ReadRestrictionsParser.toJson(restrictions);
    return Result.of(builder.build(), Optional.of(signature));
  }

  @Override
  public LoadTableResponse preserveSnapshotFilteredFields(
      LoadTableResponse original, LoadTableResponse filtered) {
    if (original == filtered) {
      return filtered;
    }
    return copyResponseBuilder(original, filtered.tableMetadata()).build();
  }

  static ReadRestrictions resolve(
      PolicyEntity[] tablePolicies,
      Map<Integer, PolicyEntity[]> columnPolicies,
      Schema schema,
      IcebergReadRestrictionConverter.BindingContext bindingContext) {
    Optional<RowFilter> rowFilter = resolveRowFilter(tablePolicies, schema, bindingContext);
    Map<Integer, ColumnMask> columnMasks =
        resolveColumnMasks(columnPolicies, schema, bindingContext);
    return IcebergReadRestrictionConverter.convert(rowFilter, columnMasks, schema, bindingContext);
  }

  static ReadRestrictions resolveForLoad(
      PolicyEntity[] tablePolicies,
      Map<Integer, PolicyEntity[]> columnPolicies,
      Schema schema,
      IcebergReadRestrictionConverter.BindingContext bindingContext) {
    try {
      return resolve(tablePolicies, columnPolicies, schema, bindingContext);
    } catch (RuntimeException exception) {
      LOG.error("Failed to resolve Iceberg read restrictions", exception);
      throw new IllegalStateException(
          "Unable to resolve read restrictions for the requested table");
    }
  }

  private static Optional<RowFilter> resolveRowFilter(
      PolicyEntity[] policies,
      Schema schema,
      IcebergReadRestrictionConverter.BindingContext bindingContext) {
    Map<String, RowFilter> distinct = new LinkedHashMap<>();
    for (PolicyEntity policy : policies) {
      if (policy.policyType() != Policy.BuiltInType.ROW_FILTER) {
        continue;
      }
      RowFilter program =
          RestrictedRegoExpressionParserFacade.parseRowFilter(
              ((RowFilterContent) policy.content()).expression());
      ReadRestrictions resolved =
          IcebergReadRestrictionConverter.convert(
              Optional.of(program), Collections.emptyMap(), schema, bindingContext);
      if (resolved.isEmpty()) {
        continue;
      }
      distinct.putIfAbsent(ReadRestrictionsParser.toJson(resolved), program);
    }
    if (distinct.size() > 1) {
      throw new IllegalArgumentException(
          "Conflicting row-filter policies apply to the requested table; only one distinct effective row-filter is supported");
    }
    return distinct.values().stream().findFirst();
  }

  private static Map<Integer, ColumnMask> resolveColumnMasks(
      Map<Integer, PolicyEntity[]> policiesByField,
      Schema schema,
      IcebergReadRestrictionConverter.BindingContext bindingContext) {
    Map<Integer, ColumnMask> resolvedMasks = new HashMap<>();
    List<Integer> fieldIds = new ArrayList<>(policiesByField.keySet());
    Collections.sort(fieldIds);
    for (Integer fieldId : fieldIds) {
      Map<String, ColumnMask> distinct = new LinkedHashMap<>();
      for (PolicyEntity policy : policiesByField.get(fieldId)) {
        if (policy.policyType() != Policy.BuiltInType.COLUMN_MASK) {
          continue;
        }
        ColumnMask program =
            RestrictedRegoExpressionParserFacade.parseColumnMask(
                ((ColumnMaskContent) policy.content()).expression());
        ReadRestrictions resolved =
            IcebergReadRestrictionConverter.convert(
                Optional.empty(),
                Collections.singletonMap(fieldId, program),
                schema,
                bindingContext);
        distinct.putIfAbsent(ReadRestrictionsParser.toJson(resolved), program);
      }
      if (distinct.size() > 1) {
        throw new IllegalArgumentException(
            "Conflicting column-mask policies apply to a requested table field");
      }
      distinct.values().stream().findFirst().ifPresent(mask -> resolvedMasks.put(fieldId, mask));
    }
    return resolvedMasks;
  }

  private static LoadTableResponse.Builder copyResponseBuilder(
      LoadTableResponse response, TableMetadata metadata) {
    LoadTableResponse.Builder builder = LoadTableResponse.builder().withTableMetadata(metadata);
    if (response.config() != null) {
      builder.addAllConfig(response.config());
    }
    if (response.credentials() != null) {
      builder.addAllCredentials(response.credentials());
    }
    if (response.remoteSigningConfig() != null) {
      builder.withRemoteSigningConfig(response.remoteSigningConfig());
    }
    if (response.labels() != null) {
      builder.withLabels(response.labels());
    }
    return builder;
  }

  static final class IdentityBindingContext
      implements IcebergReadRestrictionConverter.BindingContext {
    private final String userName;
    private final String metalake;
    private final AccessControlDispatcher accessControlDispatcher;
    private final Optional<Set<String>> groups;
    private final Map<String, Optional<Boolean>> memberships = new HashMap<>();

    IdentityBindingContext(
        String userName, String metalake, AccessControlDispatcher accessControlDispatcher) {
      this.userName = userName;
      this.metalake = metalake;
      this.accessControlDispatcher = accessControlDispatcher;
      this.groups = snapshotGroups(userName);
    }

    @Override
    public String sessionUser() {
      return userName;
    }

    @Override
    public Optional<Boolean> groupMembership(String group) {
      return memberships.computeIfAbsent(group, this::resolveGroupMembership);
    }

    private Optional<Boolean> resolveGroupMembership(String group) {
      if (!groups.isPresent() || accessControlDispatcher == null) {
        return Optional.empty();
      }
      try {
        if (accessControlDispatcher.getGroup(metalake, group) == null) {
          return Optional.empty();
        }
        return Optional.of(groups.get().contains(group));
      } catch (NoSuchGroupException exception) {
        return Optional.empty();
      } catch (RuntimeException exception) {
        LOG.warn(
            "Failed to resolve group {} in metalake {} for Iceberg read restrictions",
            group,
            metalake,
            exception);
        return Optional.empty();
      }
    }

    private static Optional<Set<String>> snapshotGroups(String userName) {
      Principal principal = PrincipalUtils.getCurrentPrincipal();
      if (!(principal instanceof UserPrincipal) || !userName.equals(principal.getName())) {
        return Optional.empty();
      }
      Set<String> result = new HashSet<>();
      for (UserGroup group : ((UserPrincipal) principal).getGroups()) {
        result.add(group.getGroupName());
      }
      return Optional.of(Collections.unmodifiableSet(result));
    }
  }
}
