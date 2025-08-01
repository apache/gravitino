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

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Audit;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.SupportsRoles;
import org.apache.gravitino.dto.model.ModelDTO;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.PolicyAlreadyAssociatedException;
import org.apache.gravitino.exceptions.TagAlreadyAssociatedException;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.SupportsPolicies;
import org.apache.gravitino.tag.SupportsTags;
import org.apache.gravitino.tag.Tag;

/** Represents a generic model. */
class GenericModel implements Model, SupportsTags, SupportsPolicies {

  private final ModelDTO modelDTO;

  private final MetadataObjectTagOperations objectTagOperations;
  private final MetadataObjectPolicyOperations objectPolicyOperations;

  GenericModel(ModelDTO modelDTO, RESTClient restClient, Namespace modelNs) {
    this.modelDTO = modelDTO;
    List<String> modelFullName =
        Lists.newArrayList(modelNs.level(1), modelNs.level(2), modelDTO.name());
    MetadataObject modelObject = MetadataObjects.of(modelFullName, MetadataObject.Type.MODEL);
    this.objectTagOperations =
        new MetadataObjectTagOperations(modelNs.level(0), modelObject, restClient);
    this.objectPolicyOperations =
        new MetadataObjectPolicyOperations(modelNs.level(0), modelObject, restClient);
  }

  @Override
  public Audit auditInfo() {
    return modelDTO.auditInfo();
  }

  @Override
  public String name() {
    return modelDTO.name();
  }

  @Override
  public String comment() {
    return modelDTO.comment();
  }

  @Override
  public Map<String, String> properties() {
    return modelDTO.properties();
  }

  @Override
  public int latestVersion() {
    return modelDTO.latestVersion();
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
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GenericModel)) {
      return false;
    }

    GenericModel that = (GenericModel) o;
    return modelDTO.equals(that.modelDTO);
  }

  @Override
  public int hashCode() {
    return modelDTO.hashCode();
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
  public String[] associateTags(String[] tagsToAdd, String[] tagsToRemove)
      throws TagAlreadyAssociatedException {
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
}
