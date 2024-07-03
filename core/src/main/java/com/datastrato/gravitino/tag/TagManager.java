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
package com.datastrato.gravitino.tag;

import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.MetadataObject;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.exceptions.NoSuchTagException;
import com.datastrato.gravitino.exceptions.TagAlreadyExistsException;
import com.datastrato.gravitino.storage.IdGenerator;
import java.io.IOException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TagManager {

  private static final Logger LOG = LoggerFactory.getLogger(TagManager.class);

  public TagManager(IdGenerator idGenerator, EntityStore entityStore) {}

  public String[] listTags(String metalake) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public Tag[] listTagsInfo(String metalake, boolean extended) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public Tag createTag(String metalake, String name, String comment, Map<String, String> properties)
      throws TagAlreadyExistsException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public Tag getTag(String metalake, String name) throws NoSuchTagException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public Tag alterTag(String metalake, String name, TagChange... changes)
      throws NoSuchTagException, IllegalArgumentException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public boolean deleteTag(String metalake, String name) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public String[] listTagsForMetadataObject(String metalake, MetadataObject metadataObject) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public Tag[] listTagsInfoForMetadataObject(String metalake, MetadataObject metadataObject) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public Tag getTagForMetadataObject(String metalake, MetadataObject metadataObject, String name)
      throws NoSuchTagException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public String[] associateTagsForMetadataObject(
      String metalake, MetadataObject metadataObject, String[] tagsToAdd, String[] tagsToRemove) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  private static void checkMetalakeExists(String metalake, EntityStore entityStore) {
    try {
      NameIdentifier metalakeIdent = NameIdentifier.of(metalake);
      if (!entityStore.exists(metalakeIdent, Entity.EntityType.METALAKE)) {
        LOG.warn("Metalake {} does not exist", metalakeIdent);
        throw new NoSuchMetalakeException("Metalake %s does not exist", metalakeIdent);
      }
    } catch (IOException ioe) {
      LOG.error("Failed to check if metalake exists", ioe);
      throw new RuntimeException(ioe);
    }
  }

  private static Namespace ofTagNamespace(String metalake) {
    return Namespace.of(metalake, Entity.SYSTEM_CATALOG_RESERVED_NAME, Entity.TAG_SCHEMA_NAME);
  }
}
