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
import org.apache.gravitino.dto.file.FilesetDTO;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.tag.SupportsTags;
import org.apache.gravitino.tag.Tag;

/** Represents a generic fileset. */
class GenericFileset implements Fileset, SupportsTags {

  private final FilesetDTO filesetDTO;

  private final MetadataObjectTagOperations objectTagOperations;

  GenericFileset(FilesetDTO filesetDTO, RESTClient restClient, Namespace filesetNs) {
    this.filesetDTO = filesetDTO;
    List<String> filesetFullName =
        Lists.newArrayList(filesetNs.level(1), filesetNs.level(2), filesetDTO.name());
    MetadataObject filesetObject = MetadataObjects.of(filesetFullName, MetadataObject.Type.FILESET);
    this.objectTagOperations =
        new MetadataObjectTagOperations(filesetNs.level(0), filesetObject, restClient);
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
  public String storageLocation() {
    return filesetDTO.storageLocation();
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
}
