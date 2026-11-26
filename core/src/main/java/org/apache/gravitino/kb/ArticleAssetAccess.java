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

package org.apache.gravitino.kb;

import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege.Name;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.meta.NamespacedEntityId;
import org.apache.gravitino.utils.PrincipalUtils;

/** Applies the ordinary asset load privileges to IDs observed in the binding operation. */
final class ArticleAssetAccess {
  private final String metalake;
  private final GravitinoAuthorizer authorizer;
  private final Principal principal;
  private final AuthorizationRequestContext context;
  private final List<MetadataObject> scopes = new ArrayList<>();

  private ArticleAssetAccess(
      String metalake, ArticleObjectIdentifier object, NamespacedEntityId ids) {
    this.metalake = metalake;
    authorizer = GravitinoEnv.getInstance().gravitinoAuthorizer();
    if (!authorizer.supportsResolvedTargetAuthorization()) {
      throw new ForbiddenException("The configured authorizer cannot check asset bindings by ID");
    }
    principal = PrincipalUtils.getCurrentPrincipal();
    Map<MetadataObject, Long> observed = new HashMap<>();
    scopes.add(MetadataObjects.of(List.of(metalake), MetadataObject.Type.METALAKE));
    List<String> names = new ArrayList<>(object.namespace());
    names.add(object.name());
    scopes.add(MetadataObjects.of(names.subList(0, 1), MetadataObject.Type.CATALOG));
    if (names.size() >= 2) {
      scopes.add(MetadataObjects.of(names.subList(0, 2), MetadataObject.Type.SCHEMA));
    }
    if (names.size() >= 3) {
      MetadataObject.Type leaf =
          object.type() == MetadataObject.Type.COLUMN ? MetadataObject.Type.TABLE : object.type();
      scopes.add(MetadataObjects.of(names.subList(0, 3), leaf));
    }
    long[] chain = ids.fullIds();
    for (int i = 0; i < scopes.size(); i++) {
      observed.put(scopes.get(i), chain[i]);
    }
    context =
        new AuthorizationRequestContext(
            metalake, scope -> Optional.ofNullable(observed.get(scope)));
  }

  static boolean readable(
      String metalake, boolean enabled, ArticleObjectIdentifier object, NamespacedEntityId ids) {
    return !enabled || new ArticleAssetAccess(metalake, object, ids).canRead();
  }

  private boolean canRead() {
    if (owns(0) || owns(1)) {
      return true;
    }
    if (!effective(Name.USE_CATALOG, 1)) {
      return false;
    }
    if (scopes.size() == 2 || owns(2)) {
      return true;
    }
    if (!effective(Name.USE_SCHEMA, 2)) {
      return false;
    }
    if (scopes.size() == 3 || owns(3)) {
      return true;
    }
    return switch (scopes.get(3).type()) {
      case TABLE -> effective(Name.SELECT_TABLE, 3) || effective(Name.MODIFY_TABLE, 3);
      case VIEW -> effective(Name.SELECT_VIEW, 3);
      case FILESET -> effective(Name.READ_FILESET, 3) || effective(Name.WRITE_FILESET, 3);
      case TOPIC -> effective(Name.CONSUME_TOPIC, 3) || effective(Name.PRODUCE_TOPIC, 3);
      case MODEL -> effective(Name.USE_MODEL, 3);
      case SEMANTIC_MODEL -> effective(Name.USE_SEMANTIC_MODEL, 3)
          || effective(Name.MODIFY_SEMANTIC_MODEL, 3);
      default -> throw new IllegalStateException("Unexpected asset scope " + scopes.get(3).type());
    };
  }

  private boolean owns(int level) {
    return authorizer.isOwner(principal, metalake, scopes.get(level), context);
  }

  private boolean effective(Name privilege, int last) {
    boolean allowed = false;
    for (MetadataObject scope : scopes.subList(0, last + 1)) {
      if (authorizer.deny(principal, metalake, scope, privilege, context)) {
        return false;
      }
      allowed = allowed || authorizer.authorize(principal, metalake, scope, privilege, context);
    }
    return allowed;
  }
}
