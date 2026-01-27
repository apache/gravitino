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

package org.apache.gravitino.server.authorization;

import java.io.IOException;
import java.security.Principal;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.authorization.AuthorizationRequestContext;
import org.apache.gravitino.authorization.GravitinoAuthorizer;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FallbackAuthorizer provides a composition-based approach to authorization by delegating to a
 * primary authorizer for Gravitino-managed objects and a secondary authorizer for external objects.
 *
 * <p>This design follows the composition pattern, allowing any two GravitinoAuthorizer
 * implementations to work together without modifications. For example:
 *
 * <ul>
 *   <li>JcasbinAuthorizer (primary) + HMSAuthorizer (secondary)
 *   <li>RangerAuthorizer (primary) + GlueAuthorizer (secondary)
 *   <li>Any authorizer combination
 * </ul>
 *
 * <p><b>How it works:</b>
 *
 * <ol>
 *   <li>Check if object exists in Gravitino EntityStore
 *   <li>If exists → delegate to primary authorizer
 *   <li>If not exists → delegate to secondary authorizer
 * </ol>
 *
 * <p><b>Configuration:</b>
 *
 * <pre>
 * gravitino.authorization.impl = org.apache.gravitino.server.authorization.FallbackAuthorizer
 * gravitino.authorization.fallback.primary = org.apache.gravitino.server.authorization.jcasbin.JcasbinAuthorizer
 * gravitino.authorization.fallback.secondary = com.example.HMSAuthorizer
 * </pre>
 */
public class FallbackAuthorizer implements GravitinoAuthorizer {

  private static final Logger LOG = LoggerFactory.getLogger(FallbackAuthorizer.class);

  private GravitinoAuthorizer primaryAuthorizer;
  private GravitinoAuthorizer secondaryAuthorizer;

  @Override
  public void initialize() {
    // Authorizers will be injected via setters or constructor
    // Configuration handling will be done by GravitinoAuthorizerProvider
    if (primaryAuthorizer != null) {
      primaryAuthorizer.initialize();
    }
    if (secondaryAuthorizer != null) {
      secondaryAuthorizer.initialize();
    }
  }

  /**
   * Set the primary authorizer for Gravitino-managed objects.
   *
   * @param authorizer The primary authorizer
   */
  public void setPrimaryAuthorizer(GravitinoAuthorizer authorizer) {
    this.primaryAuthorizer = authorizer;
  }

  /**
   * Set the secondary authorizer for external objects.
   *
   * @param authorizer The secondary authorizer
   */
  public void setSecondaryAuthorizer(GravitinoAuthorizer authorizer) {
    this.secondaryAuthorizer = authorizer;
  }

  @Override
  public boolean authorize(
      Principal principal,
      String metalake,
      MetadataObject metadataObject,
      Privilege.Name privilege,
      AuthorizationRequestContext requestContext) {

    if (existsInGravitino(metalake, metadataObject)) {
      LOG.debug(
          "Object {} exists in Gravitino, using primary authorizer", metadataObject.fullName());
      return primaryAuthorizer.authorize(
          principal, metalake, metadataObject, privilege, requestContext);
    } else {
      LOG.debug(
          "Object {} not found in Gravitino, using secondary authorizer",
          metadataObject.fullName());
      if (secondaryAuthorizer != null) {
        return secondaryAuthorizer.authorize(
            principal, metalake, metadataObject, privilege, requestContext);
      }
      // No secondary authorizer configured - allow operation to proceed
      // so natural 404 occurs during entity loading
      return true;
    }
  }

  @Override
  public boolean deny(
      Principal principal,
      String metalake,
      MetadataObject metadataObject,
      Privilege.Name privilege,
      AuthorizationRequestContext requestContext) {

    if (existsInGravitino(metalake, metadataObject)) {
      return primaryAuthorizer.deny(principal, metalake, metadataObject, privilege, requestContext);
    } else {
      // For external objects, secondary system handles holistic authorization
      // No separate deny check needed
      return false;
    }
  }

  @Override
  public boolean isOwner(
      Principal principal,
      String metalake,
      MetadataObject metadataObject,
      AuthorizationRequestContext requestContext) {

    if (existsInGravitino(metalake, metadataObject)) {
      return primaryAuthorizer.isOwner(principal, metalake, metadataObject, requestContext);
    } else {
      if (secondaryAuthorizer != null) {
        return secondaryAuthorizer.isOwner(principal, metalake, metadataObject, requestContext);
      }
      return true;
    }
  }

  @Override
  public boolean isServiceAdmin() {
    return primaryAuthorizer.isServiceAdmin();
  }

  @Override
  public boolean isSelf(Entity.EntityType type, NameIdentifier nameIdentifier) {
    return primaryAuthorizer.isSelf(type, nameIdentifier);
  }

  @Override
  public boolean isMetalakeUser(String metalake) {
    return primaryAuthorizer.isMetalakeUser(metalake);
  }

  @Override
  public boolean hasSetOwnerPermission(
      String metalake, String type, String fullName, AuthorizationRequestContext requestContext) {
    return primaryAuthorizer.hasSetOwnerPermission(metalake, type, fullName, requestContext);
  }

  @Override
  public boolean hasMetadataPrivilegePermission(
      String metalake, String type, String fullName, AuthorizationRequestContext requestContext) {
    return primaryAuthorizer.hasMetadataPrivilegePermission(
        metalake, type, fullName, requestContext);
  }

  @Override
  public void handleRolePrivilegeChange(Long roleId) {
    primaryAuthorizer.handleRolePrivilegeChange(roleId);
  }

  @Override
  public void handleMetadataOwnerChange(
      String metalake, Long oldOwnerId, NameIdentifier nameIdentifier, Entity.EntityType type) {
    primaryAuthorizer.handleMetadataOwnerChange(metalake, oldOwnerId, nameIdentifier, type);
  }

  @Override
  public void close() throws IOException {
    if (primaryAuthorizer != null) {
      primaryAuthorizer.close();
    }
    if (secondaryAuthorizer != null) {
      secondaryAuthorizer.close();
    }
  }

  /**
   * Check if a metadata object exists in Gravitino EntityStore.
   *
   * @param metalake The metalake name
   * @param metadataObject The metadata object to check
   * @return true if object exists in Gravitino, false otherwise
   */
  private boolean existsInGravitino(String metalake, MetadataObject metadataObject) {
    try {
      MetadataIdConverter.getID(metadataObject, metalake);
      return true;
    } catch (NoSuchEntityException e) {
      LOG.debug("Object {} not found in Gravitino EntityStore", metadataObject.fullName());
      return false;
    } catch (Exception e) {
      LOG.warn(
          "Error checking if object {} exists in Gravitino, assuming it doesn't exist",
          metadataObject.fullName(),
          e);
      return false;
    }
  }
}
