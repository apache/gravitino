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

import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.iceberg.service.IcebergRESTUtils;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata.RequestType;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.RESTUtil;

/**
 * Through dynamic proxy, obtain the annotations on the method and parameter list to perform
 * metadata authorization.
 */
public class IcebergMetadataAuthorizationMethodInterceptor
    extends BaseMetadataAuthorizationMethodInterceptor {

  private final String metalakeName = IcebergRESTServerContext.getInstance().metalakeName();

  @Override
  protected Map<Entity.EntityType, NameIdentifier> extractNameIdentifierFromParameters(
      Parameter[] parameters, Object[] args) {
    Map<Entity.EntityType, NameIdentifier> nameIdentifierMap = new HashMap<>();
    nameIdentifierMap.put(Entity.EntityType.METALAKE, NameIdentifierUtil.ofMetalake(metalakeName));
    // get catalog & namespace from params
    String catalog = null;
    String schema = null;
    Namespace rawNamespace = null;
    for (int i = 0; i < parameters.length; i++) {
      Parameter parameter = parameters[i];
      AuthorizationMetadata authorizeResource =
          parameter.getAnnotation(AuthorizationMetadata.class);
      if (authorizeResource == null) {
        continue;
      }
      Entity.EntityType type = authorizeResource.type();
      String value = String.valueOf(args[i]);
      switch (type) {
        case CATALOG:
          catalog = IcebergRESTUtils.getCatalogName(value);
          nameIdentifierMap.put(
              Entity.EntityType.CATALOG, NameIdentifierUtil.ofCatalog(metalakeName, catalog));
          break;
        case SCHEMA:
          rawNamespace = RESTUtil.decodeNamespace(value);
          schema = rawNamespace.level(rawNamespace.length() - 1);
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

  /**
   * Creates an authorization handler for Iceberg-specific operations that require custom logic
   * beyond standard annotation-based authorization.
   */
  @Override
  protected Optional<AuthorizationHandler> createAuthorizationHandler(
      Parameter[] parameters, Object[] args) {
    for (Parameter parameter : parameters) {
      IcebergAuthorizationMetadata icebergMetadata =
          parameter.getAnnotation(IcebergAuthorizationMetadata.class);
      if (icebergMetadata != null) {
        // Create handler with parameters and args for processing
        RequestType type = icebergMetadata.type();
        switch (type) {
          case LOAD_TABLE:
            return Optional.of(new LoadTableAuthzHandler(parameters, args));
          case RENAME_TABLE:
            return Optional.of(new RenameTableAuthzHandler(parameters, args));
          default:
            break;
        }
      }
    }
    return Optional.empty();
  }

  @Override
  protected boolean isExceptionPropagate(Exception e) {
    return e.getClass().getName().startsWith("org.apache.iceberg.exceptions");
  }
}
