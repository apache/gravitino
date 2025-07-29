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
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.SupportsRoles;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.SupportsCredentials;
import org.apache.gravitino.dto.file.FilesetDTO;
import org.apache.gravitino.exceptions.NoSuchPolicyException;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.exceptions.PolicyAlreadyAssociatedException;
import org.apache.gravitino.file.Fileset;
<<<<<<< HEAD
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.SupportsPolicies;
=======
import org.apache.gravitino.stats.Statistic;
import org.apache.gravitino.stats.StatisticValue;
import org.apache.gravitino.stats.SupportsStatistics;
>>>>>>> dc6f26d92 ([#7274] feat(client-java): Support statistics http client config for gravitino client)
import org.apache.gravitino.tag.SupportsTags;
import org.apache.gravitino.tag.Tag;

/** Represents a generic fileset. */
class GenericFileset
<<<<<<< HEAD
    implements Fileset, SupportsTags, SupportsRoles, SupportsCredentials, SupportsPolicies {
=======
    implements Fileset, SupportsTags, SupportsRoles, SupportsCredentials, SupportsStatistics {
>>>>>>> dc6f26d92 ([#7274] feat(client-java): Support statistics http client config for gravitino client)

  private final FilesetDTO filesetDTO;

  private final MetadataObjectTagOperations objectTagOperations;
  private final MetadataObjectRoleOperations objectRoleOperations;
  private final MetadataObjectCredentialOperations objectCredentialOperations;
<<<<<<< HEAD
  private final MetadataObjectPolicyOperations objectPolicyOperations;
=======
  private final MetadataObjectStatisticsOperations objectStatisticsOperations;
>>>>>>> dc6f26d92 ([#7274] feat(client-java): Support statistics http client config for gravitino client)

  GenericFileset(FilesetDTO filesetDTO, RESTClient restClient, Namespace filesetNs) {
    this.filesetDTO = filesetDTO;
    List<String> filesetFullName =
        Lists.newArrayList(filesetNs.level(1), filesetNs.level(2), filesetDTO.name());
    MetadataObject filesetObject = MetadataObjects.of(filesetFullName, MetadataObject.Type.FILESET);
    this.objectTagOperations =
        new MetadataObjectTagOperations(filesetNs.level(0), filesetObject, restClient);
    this.objectRoleOperations =
        new MetadataObjectRoleOperations(filesetNs.level(0), filesetObject, restClient);
    this.objectCredentialOperations =
        new MetadataObjectCredentialOperations(filesetNs.level(0), filesetObject, restClient);
<<<<<<< HEAD
    this.objectPolicyOperations =
        new MetadataObjectPolicyOperations(filesetNs.level(0), filesetObject, restClient);
=======
    this.objectStatisticsOperations =
        new MetadataObjectStatisticsOperations(
            filesetNs.level(0), filesetNs.level(1), filesetObject, restClient);
>>>>>>> dc6f26d92 ([#7274] feat(client-java): Support statistics http client config for gravitino client)
  }

  @Override
  public Audit auditInfo() {
    return filesetDTO.auditInfo();
  }

  @Override
  public String name() {
    return filesetDTO.name();
  }

  @Nullable
  @Override
  public String comment() {
    return filesetDTO.comment();
  }

  @Override
  public Type type() {
    return filesetDTO.type();
  }

  @Override
  public Map<String, String> storageLocations() {
    return filesetDTO.storageLocations();
  }

  @Override
  public Map<String, String> properties() {
    return filesetDTO.properties();
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
  public SupportsCredentials supportsCredentials() {
    return this;
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
  public Credential[] getCredentials() {
    return objectCredentialOperations.getCredentials();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof GenericFileset)) {
      return false;
    }

    GenericFileset that = (GenericFileset) obj;
    return filesetDTO.equals(that.filesetDTO);
  }

  @Override
  public int hashCode() {
    return filesetDTO.hashCode();
  }

  @Override
  public List<Statistic> listStatistics() {
    return objectStatisticsOperations.listStatistics();
  }

  @Override
  public List<Statistic> updateStatistics(Map<String, StatisticValue<?>> statistics) {
    return objectStatisticsOperations.updateStatistics(statistics);
  }

  @Override
  public boolean dropStatistics(List<String> statistics) {
    return objectStatisticsOperations.dropStatistics(statistics);
  }
}
