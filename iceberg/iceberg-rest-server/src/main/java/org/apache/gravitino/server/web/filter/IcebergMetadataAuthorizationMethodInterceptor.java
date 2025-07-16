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

package org.apache.gravitino.server.web.filter;

import com.google.common.base.Preconditions;
import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.iceberg.service.IcebergRestUtils;
import org.apache.gravitino.iceberg.service.authorization.IcebergAuthorizationContext;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.requests.CreateTableRequest;

/**
 * Through dynamic proxy, obtain the annotations on the method and parameter list to perform
 * metadata authorization.
 */
public class IcebergMetadataAuthorizationMethodInterceptor
    extends BaseMetadataAuthorizationMethodInterceptor {
  private final String metalakeName = IcebergAuthorizationContext.getInstance().metalakeName();

  @Override
  Map<Entity.EntityType, NameIdentifier> extractNameIdentifierFromParameters(
      Parameter[] parameters, Object[] args) {
    Map<Entity.EntityType, NameIdentifier> nameIdentifierMap = new HashMap<>();
    nameIdentifierMap.put(Entity.EntityType.METALAKE, NameIdentifierUtil.ofMetalake(metalakeName));
    // get catalog & namespace from params
    String catalog = null;
    String schema = null;
    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];
      AuthorizationMetadata authorizeResource =
          parameter.getAnnotation(AuthorizationMetadata.class);
      IcebergAuthorizationMetadata icebergAuthorizeResource =
          parameter.getAnnotation(IcebergAuthorizationMetadata.class);
      if (icebergAuthorizeResource != null) {
        switch (icebergAuthorizeResource.type()) {
          case CREATE_TABLE:
            CreateTableRequest request = (CreateTableRequest) args[i];
            nameIdentifierMap.put(
                Entity.EntityType.TABLE,
                NameIdentifierUtil.ofTable(metalakeName, catalog, schema, request.name()));
            break;
          default:
            break;
        }
      }
      if (authorizeResource == null) {
        continue;
      }
      MetadataObject.Type type = authorizeResource.type();
      String value = String.valueOf(args[i]);
      switch (type) {
        case CATALOG:
          catalog = IcebergRestUtils.getCatalogName(value);
          nameIdentifierMap.put(
              Entity.EntityType.CATALOG, NameIdentifierUtil.ofCatalog(metalakeName, catalog));
          break;
        case SCHEMA:
          Namespace ns = RESTUtil.decodeNamespace(value);
          Preconditions.checkArgument(
              ns.levels().length == 1, "Gravitino auth doesn't support multiple namespace: %s", ns);
          schema = ns.level(0);
          nameIdentifierMap.put(
              Entity.EntityType.SCHEMA, NameIdentifierUtil.ofSchema(metalakeName, catalog, schema));
          break;
        case TABLE:
          nameIdentifierMap.put(
              EntityType.TABLE, NameIdentifierUtil.ofTable(metalakeName, catalog, schema, value));
          break;
        default:
          break;
      }
    }
    return nameIdentifierMap;
  }
}
