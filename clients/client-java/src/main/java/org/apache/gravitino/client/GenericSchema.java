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
package org.apache.gravitino.client;

import java.util.Map;
import org.apache.gravitino.Audit;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.Schema;
import org.apache.gravitino.authorization.SupportsRoles;
import org.apache.gravitino.dto.SchemaDTO;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.PolicyAlreadyAssociatedException;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.SupportsPolicies;
import org.apache.gravitino.tag.SupportsTags;
import org.apache.gravitino.tag.Tag;

/** Represents a generic schema. */
class GenericSchema implements Schema, SupportsTags, SupportsRoles, SupportsPolicies {

  private final SchemaDTO schemaDTO;

  private final MetadataObjectTagOperations objectTagOperations;
  private final MetadataObjectRoleOperations objectRoleOperations;
  private final MetadataObjectPolicyOperations objectPolicyOperations;

  GenericSchema(SchemaDTO schemaDTO, RESTClient restClient, String metalake, String catalog) {
    this.schemaDTO = schemaDTO;
    MetadataObject schemaObject =
        MetadataObjects.of(catalog, schemaDTO.name(), MetadataObject.Type.SCHEMA);
    this.objectTagOperations = new MetadataObjectTagOperations(metalake, schemaObject, restClient);
    this.objectRoleOperations =
        new MetadataObjectRoleOperations(metalake, schemaObject, restClient);
    this.objectPolicyOperations =
        new MetadataObjectPolicyOperations(metalake, schemaObject, restClient);
  }

  @Override
  public SupportsTags supportsTags() {
    return this;
  }

  @Override
  public SupportsPolicies supportsPolicies() {
    return this;
  }

  @Override
  public SupportsRoles supportsRoles() {
    return this;
  }

  @Override
  public String name() {
    return schemaDTO.name();
  }

  @Override
  public String comment() {
    return schemaDTO.comment();
  }

  @Override
  public Map<String, String> properties() {
    return schemaDTO.properties();
  }

  @Override
  public Audit auditInfo() {
    return schemaDTO.auditInfo();
  }

  @Override
  public String[] listTags() {
    return objectTagOperations.listTags();
  }

  @Override
  public Tag[] listTagsInfo() {
    return objectTagOperations.listTagsInfo();
  }

  @Override
  public Tag getTag(String name) throws NoSuchTagException {
    return objectTagOperations.getTag(name);
  }

  @Override
  public String[] associateTags(String[] tagsToAdd, String[] tagsToRemove) {
    return objectTagOperations.associateTags(tagsToAdd, tagsToRemove);
  }

  @Override
  public String[] listPolicies() {
    return objectPolicyOperations.listPolicies();
  }

  @Override
  public Policy[] listPolicyInfos() {
    return objectPolicyOperations.listPolicyInfos();
  }

  @Override
  public Policy getPolicy(String name) throws NoSuchPolicyException {
    return objectPolicyOperations.getPolicy(name);
  }

  @Override
  public String[] associatePolicies(String[] policiesToAdd, String[] policiesToRemove)
      throws PolicyAlreadyAssociatedException {
    return objectPolicyOperations.associatePolicies(policiesToAdd, policiesToRemove);
  }

  @Override
  public String[] listBindingRoleNames() {
    return objectRoleOperations.listBindingRoleNames();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof GenericSchema)) {
      return false;
    }

    GenericSchema that = (GenericSchema) obj;
    return schemaDTO.equals(that.schemaDTO);
  }

  @Override
  public int hashCode() {
    return schemaDTO.hashCode();
  }
}
