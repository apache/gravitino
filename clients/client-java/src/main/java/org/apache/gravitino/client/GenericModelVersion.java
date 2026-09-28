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

import java.util.Arrays;
import java.util.Map;
import org.apache.gravitino.Audit;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.dto.model.ModelVersionDTO;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.secret.SupportsSecrets;

class GenericModelVersion implements ModelVersion, SupportsSecrets {

  private final ModelVersionDTO modelVersionDTO;

  private final MetadataObjectSecretOperations objectSecretOperations;

  GenericModelVersion(
      ModelVersionDTO modelVersionDTO, RESTClient restClient, NameIdentifier modelFullIdent) {
    this.modelVersionDTO = modelVersionDTO;
    MetadataObject modelVersionObject =
        MetadataObjects.of(
            Arrays.asList(
                modelFullIdent.namespace().level(1),
                modelFullIdent.namespace().level(2),
                modelFullIdent.name(),
                String.valueOf(modelVersionDTO.version())),
            MetadataObject.Type.MODEL_VERSION);
    this.objectSecretOperations =
        new MetadataObjectSecretOperations(
            modelFullIdent.namespace().level(0), modelVersionObject, restClient);
  }

  @Override
  public int version() {
    return modelVersionDTO.version();
  }

  @Override
  public Map<String, String> uris() {
    return modelVersionDTO.uris();
  }

  @Override
  public String comment() {
    return modelVersionDTO.comment();
  }

  @Override
  public String[] aliases() {
    return modelVersionDTO.aliases();
  }

  @Override
  public Map<String, String> properties() {
    return modelVersionDTO.properties();
  }

  @Override
  public Audit auditInfo() {
    return modelVersionDTO.auditInfo();
  }

  @Override
  public SupportsSecrets supportsSecrets() {
    return this;
  }

  @Override
  public Map<String, String> getSecrets() {
    return objectSecretOperations.getSecrets();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof GenericModelVersion)) {
      return false;
    }
    GenericModelVersion that = (GenericModelVersion) o;
    return modelVersionDTO.equals(that.modelVersionDTO);
  }

  @Override
  public int hashCode() {
    return modelVersionDTO.hashCode();
  }
}
