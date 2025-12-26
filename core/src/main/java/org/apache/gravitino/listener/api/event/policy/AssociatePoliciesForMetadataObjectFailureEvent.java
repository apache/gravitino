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

package org.apache.gravitino.listener.api.event.policy;

import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.utils.MetadataObjectUtil;

/**
 * Represents an event that is triggered when an attempt to associate policies with a metadata
 * object fails.
 */
@DeveloperApi
public final class AssociatePoliciesForMetadataObjectFailureEvent extends PolicyFailureEvent {
  private final MetadataObject metadataObject;
  private final String[] policiesToAdd;
  private final String[] policiesToRemove;

  /**
   * Constructs an instance of {@code AssociatePoliciesForMetadataObjectFailureEvent}.
   *
   * @param user The username of the individual who initiated the associate policies operation.
   * @param metalake The metalake from which the policies were to be associated.
   * @param metadataObject The metadata object with which the policies were to be associated.
   * @param policiesToAdd The policies to be added.
   * @param policiesToRemove The policies to be removed.
   * @param exception The exception that was encountered during the associate policies attempt.
   */
  public AssociatePoliciesForMetadataObjectFailureEvent(
      String user,
      String metalake,
      MetadataObject metadataObject,
      String[] policiesToAdd,
      String[] policiesToRemove,
      Exception exception) {
    super(user, MetadataObjectUtil.toEntityIdent(metalake, metadataObject), exception);
    this.metadataObject = metadataObject;
    this.policiesToAdd = policiesToAdd != null ? policiesToAdd : new String[0];
    this.policiesToRemove = policiesToRemove != null ? policiesToRemove : new String[0];
  }

  /**
   * Returns the metadata object with which policies were to be associated.
   *
   * @return the metadata object.
   */
  public MetadataObject metadataObject() {
    return metadataObject;
  }

  /**
   * Returns the policies to be added.
   *
   * @return an array of policy names to be added.
   */
  public String[] policiesToAdd() {
    return policiesToAdd;
  }

  /**
   * Returns the policies to be removed.
   *
   * @return an array of policy names to be removed.
   */
  public String[] policiesToRemove() {
    return policiesToRemove;
  }

  /**
   * Returns the type of operation.
   *
   * @return the operation type.
   */
  @Override
  public OperationType operationType() {
    return OperationType.ASSOCIATE_POLICIES_FOR_METADATA_OBJECT;
  }
}
