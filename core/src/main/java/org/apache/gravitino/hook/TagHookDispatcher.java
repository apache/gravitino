/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.hook;

import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagChange;
import org.apache.gravitino.tag.TagDispatcher;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.PrincipalUtils;

public class TagHookDispatcher implements TagDispatcher {

  private final TagDispatcher dispatcher;

  public TagHookDispatcher(TagDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public String[] listTags(String metalake) {
    return dispatcher.listTags(metalake);
  }

  @Override
  public Tag[] listTagsInfo(String metalake) {
    return dispatcher.listTagsInfo(metalake);
  }

  @Override
  public Tag getTag(String metalake, String name) throws NoSuchTagException {
    return dispatcher.getTag(metalake, name);
  }

  @Override
  public Tag createTag(
      String metalake, String name, String comment, Map<String, String> properties) {
    AuthorizationUtils.checkCurrentUser(metalake, PrincipalUtils.getCurrentUserName());
    Tag tag = dispatcher.createTag(metalake, name, comment, properties);

    // Set the creator as the owner of the catalog.
    OwnerDispatcher ownerDispatcher = GravitinoEnv.getInstance().ownerDispatcher();
    if (ownerDispatcher != null) {
      ownerDispatcher.setOwner(
          metalake,
          NameIdentifierUtil.toMetadataObject(
              NameIdentifierUtil.ofTag(metalake, name), Entity.EntityType.TAG),
          PrincipalUtils.getCurrentUserName(),
          Owner.Type.USER);
    }

    return tag;
  }

  @Override
  public Tag alterTag(String metalake, String name, TagChange... changes) {
    return dispatcher.alterTag(metalake, name, changes);
  }

  @Override
  public boolean deleteTag(String metalake, String name) {
    return dispatcher.deleteTag(metalake, name);
  }

  @Override
  public MetadataObject[] listMetadataObjectsForTag(String metalake, String name) {
    return dispatcher.listMetadataObjectsForTag(metalake, name);
  }

  @Override
  public String[] listTagsForMetadataObject(String metalake, MetadataObject metadataObject) {
    return dispatcher.listTagsForMetadataObject(metalake, metadataObject);
  }

  @Override
  public Tag[] listTagsInfoForMetadataObject(String metalake, MetadataObject metadataObject) {
    return dispatcher.listTagsInfoForMetadataObject(metalake, metadataObject);
  }

  @Override
  public String[] associateTagsForMetadataObject(
      String metalake, MetadataObject metadataObject, String[] tagsToAdd, String[] tagsToRemove) {
    return dispatcher.associateTagsForMetadataObject(
        metalake, metadataObject, tagsToAdd, tagsToRemove);
  }

  @Override
  public Tag getTagForMetadataObject(String metalake, MetadataObject metadataObject, String name) {
    return dispatcher.getTagForMetadataObject(metalake, metadataObject, name);
  }
}
