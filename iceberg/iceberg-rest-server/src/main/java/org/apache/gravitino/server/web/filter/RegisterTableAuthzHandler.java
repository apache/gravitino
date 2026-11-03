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

import static org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants.LOAD_TABLE_AUTHORIZATION_EXPRESSION;

import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.iceberg.service.CatalogWrapperForREST;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.dispatcher.RegisterTableSourceResolver;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionEvaluator;
import org.apache.gravitino.server.web.filter.BaseMetadataAuthorizationMethodInterceptor.AuthorizationHandler;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.requests.RegisterTableRequest;

/**
 * Enriches Iceberg REST {@code registerTable} authorization with the named table identifier and
 * dynamic source-table checks.
 *
 * <p>Create-versus-overwrite expression selection stays in the shared authorization pipeline via
 * {@code secondaryExpression} / {@code secondaryExpressionCondition}. This handler only adds the
 * TABLE identifier needed for overwrite evaluation and requires the load-table expression when the
 * metadata file still belongs to another live table, so {@code CREATE_TABLE} alone cannot adopt an
 * unreadable table's metadata.
 */
@SuppressWarnings("FormatStringAnnotation")
public class RegisterTableAuthzHandler implements AuthorizationHandler {

  private static final Pattern SCHEMA_SPLIT =
      Pattern.compile(Pattern.quote(HierarchicalSchemaUtil.schemaSeparator()));

  private final Parameter[] parameters;
  private final Object[] args;

  /**
   * Creates a handler for the register-table request arguments.
   *
   * @param parameters invoked method parameters
   * @param args invoked method arguments
   */
  public RegisterTableAuthzHandler(Parameter[] parameters, Object[] args) {
    this.parameters = parameters;
    this.args = args;
  }

  @Override
  public void process(Map<EntityType, NameIdentifier> nameIdentifierMap) {
    RegisterTableRequest request = registerTableRequest();

    NameIdentifier metalakeIdent = nameIdentifierMap.get(EntityType.METALAKE);
    NameIdentifier catalogIdent = nameIdentifierMap.get(EntityType.CATALOG);
    NameIdentifier schemaIdent = nameIdentifierMap.get(EntityType.SCHEMA);
    if (metalakeIdent == null || catalogIdent == null || schemaIdent == null) {
      throw new ForbiddenException(
          "Missing metalake, catalog, or schema context for register-table authorization");
    }

    String metalakeName = metalakeIdent.name();
    String catalog = catalogIdent.name();
    String schema = schemaIdent.name();
    String tableName = request.name();
    nameIdentifierMap.put(
        EntityType.TABLE, NameIdentifierUtil.ofTable(metalakeName, catalog, schema, tableName));

    authorizeMetadataSources(metalakeName, catalog, schema, tableName, request);
  }

  @Override
  public boolean authorizationCompleted() {
    // Create/overwrite expression evaluation stays in the standard authorization pipeline.
    return false;
  }

  private void authorizeMetadataSources(
      String metalakeName,
      String catalog,
      String schema,
      String tableName,
      RegisterTableRequest request) {
    CatalogWrapperForREST catalogWrapper =
        IcebergRESTServerContext.getInstance().catalogWrapperManager().getCatalogWrapper(catalog);
    TableMetadata metadata =
        catalogWrapper.loadTableMetadataFromLocation(request.metadataLocation());
    Namespace targetNamespace = Namespace.of(SCHEMA_SPLIT.split(schema, -1));
    TableIdentifier target = TableIdentifier.of(targetNamespace, tableName);
    List<TableIdentifier> sources =
        RegisterTableSourceResolver.findSourceTables(
            catalogWrapper, target, request.metadataLocation(), metadata);
    for (TableIdentifier source : sources) {
      requireCanLoadTable(metalakeName, catalog, source);
    }
  }

  private static void requireCanLoadTable(
      String metalake, String catalogName, TableIdentifier source) {
    String sourceSchema =
        String.join(HierarchicalSchemaUtil.schemaSeparator(), source.namespace().levels());
    Map<EntityType, NameIdentifier> identifiers = new HashMap<>();
    identifiers.put(EntityType.METALAKE, NameIdentifierUtil.ofMetalake(metalake));
    identifiers.put(EntityType.CATALOG, NameIdentifierUtil.ofCatalog(metalake, catalogName));
    identifiers.put(
        EntityType.SCHEMA, NameIdentifierUtil.ofSchema(metalake, catalogName, sourceSchema));
    identifiers.put(
        EntityType.TABLE,
        NameIdentifierUtil.ofTable(metalake, catalogName, sourceSchema, source.name()));
    boolean authorized =
        new AuthorizationExpressionEvaluator(LOAD_TABLE_AUTHORIZATION_EXPRESSION)
            .evaluate(
                identifiers,
                new HashMap<>(),
                new AuthorizationRequestContext(),
                Optional.of(EntityType.TABLE.name()));
    if (!authorized) {
      throw new ForbiddenException(
          "User '%s' is not authorized to register metadata from table '%s'",
          PrincipalUtils.getCurrentUserName(), source);
    }
  }

  private RegisterTableRequest registerTableRequest() {
    for (int i = 0; i < parameters.length; i++) {
      IcebergAuthorizationMetadata metadata =
          parameters[i].getAnnotation(IcebergAuthorizationMetadata.class);
      if (metadata != null
          && metadata.type() == IcebergAuthorizationMetadata.RequestType.REGISTER_TABLE) {
        return (RegisterTableRequest) args[i];
      }
    }
    throw new ForbiddenException("RegisterTableRequest not found in parameters");
  }
}
