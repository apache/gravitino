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
package org.apache.gravitino.server.web.rest;

import com.google.common.base.Preconditions;
import java.util.Locale;
import java.util.function.Supplier;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.AlreadyExistsException;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.InUseException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.exceptions.NotInUseException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.rest.RESTRequest;
import org.apache.gravitino.semantic.OssieDocument;
import org.apache.gravitino.semantic.OssieFormat;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.web.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Shared handling for ontology and mapping requests. */
final class OntologyRestSupport {
  static final String YAML = "application/yaml";
  static final String VND_GRAVITINO_V1_JSON = "application/vnd.gravitino.v1+json";
  private static final Logger LOG = LoggerFactory.getLogger(OntologyRestSupport.class);

  private OntologyRestSupport() {}

  static Response run(HttpServletRequest request, Supplier<Response> operation) {
    try {
      return Utils.doAs(request, operation::get);
    } catch (IllegalArgumentException e) {
      return Utils.illegalArguments(e.getMessage(), e);
    } catch (NotFoundException e) {
      return Utils.notFound(e.getMessage(), e);
    } catch (AlreadyExistsException e) {
      return Utils.alreadyExists(e.getMessage(), e);
    } catch (InUseException e) {
      return Utils.inUse(e.getMessage(), e);
    } catch (NotInUseException e) {
      return Utils.notInUse(e.getMessage(), e);
    } catch (ForbiddenException e) {
      return Utils.forbidden(e.getMessage(), e);
    } catch (OptimisticLockException e) {
      LOG.warn("Ontology operation conflicted with a concurrent write", e);
      return Utils.optimisticLockConflict(e.getMessage(), e);
    } catch (Exception e) {
      LOG.error("Ontology operation failed", e);
      return Utils.internalError("Ontology operation failed", e);
    }
  }

  static void validate(RESTRequest request) {
    Preconditions.checkArgument(request != null, "Request body is required");
    request.validate();
  }

  static OssieDocument document(String content, HttpHeaders headers) {
    MediaType media = headers.getMediaType();
    return media != null && MediaType.APPLICATION_JSON_TYPE.isCompatible(media)
        ? OssieDocument.json(content)
        : OssieDocument.yaml(content);
  }

  static OssieFormat format(String format) {
    try {
      return OssieFormat.valueOf(format.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Unsupported format: " + format + ". Expected yaml or json.", e);
    }
  }

  static Response exported(OssieDocument document) {
    return Response.ok(
            document.content(),
            document.format() == OssieFormat.JSON ? MediaType.APPLICATION_JSON : YAML)
        .build();
  }

  static boolean canLoadMapping(NameIdentifier mapping) {
    NameIdentifier model = NameIdentifier.of(mapping.namespace().levels());
    return MetadataAuthzHelper.checkAccess(
        model,
        Entity.EntityType.SEMANTIC_MODEL,
        AuthorizationExpressionConstants.LOAD_SEMANTIC_MODEL_AUTHORIZATION_EXPRESSION);
  }
}
