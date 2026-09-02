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
package org.apache.gravitino.lance.service.authorization;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.lance.common.ops.LanceMetadataFilter;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** Removes the catalogs, schemas, and tables the caller may not see from a Lance listing. */
public class LanceAuthorizationMetadataFilter implements LanceMetadataFilter {

  private final String metalakeName;

  /**
   * Creates a filter bound to a metalake.
   *
   * @param metalakeName the metalake Lance REST is bound to.
   */
  public LanceAuthorizationMetadataFilter(String metalakeName) {
    this.metalakeName = metalakeName;
  }

  @Override
  public List<String> filterCatalogs(List<String> catalogNames) {
    return filter(
        catalogNames,
        Entity.EntityType.CATALOG,
        AuthorizationExpressionConstants.LOAD_CATALOG_AUTHORIZATION_EXPRESSION,
        name -> NameIdentifierUtil.ofCatalog(metalakeName, name));
  }

  @Override
  public List<String> filterSchemas(String catalogName, List<String> schemaNames) {
    return filter(
        schemaNames,
        Entity.EntityType.SCHEMA,
        AuthorizationExpressionConstants.FILTER_SCHEMA_AUTHORIZATION_EXPRESSION,
        name -> NameIdentifierUtil.ofSchema(metalakeName, catalogName, name));
  }

  @Override
  public List<String> filterTables(String catalogName, String schemaName, List<String> tableNames) {
    return filter(
        tableNames,
        Entity.EntityType.TABLE,
        AuthorizationExpressionConstants.FILTER_TABLE_AUTHORIZATION_EXPRESSION,
        name -> NameIdentifierUtil.ofTable(metalakeName, catalogName, schemaName, name));
  }

  private List<String> filter(
      List<String> names,
      Entity.EntityType entityType,
      String expression,
      Function<String, NameIdentifier> toIdentifier) {
    NameIdentifier[] identifiers = names.stream().map(toIdentifier).toArray(NameIdentifier[]::new);
    NameIdentifier[] authorized =
        MetadataAuthzHelper.filterByExpression(metalakeName, expression, entityType, identifiers);
    return Arrays.stream(authorized).map(NameIdentifier::name).collect(Collectors.toList());
  }
}
