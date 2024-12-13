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
package org.apache.gravitino.listener;

import java.util.Map;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.exceptions.NoSuchTagException;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagChange;
import org.apache.gravitino.tag.TagDispatcher;

public class TagEventDispatcher implements TagDispatcher {
  @SuppressWarnings("unused")
  private final EventBus eventBus;

  @SuppressWarnings("unused")
  private final TagDispatcher dispatcher;

  public TagEventDispatcher(EventBus eventBus, TagDispatcher dispatcher) {
    this.eventBus = eventBus;
    this.dispatcher = dispatcher;
  }

  @Override
  public String[] listTags(String metalake) {
    // TODO: listTagsPreEvent
    try {
      // TODO: listTagsEvent
    } catch (Exception e) {
      // TODO: listTagFailureEvent
      throw e;
    }
    return dispatcher.listTags(metalake);
  }

  @Override
  public Tag[] listTagsInfo(String metalake) {
    // TODO: listTagsInfoPreEvent
    try {
      // TODO: listTagsInfoEvent
    } catch (Exception e) {
      // TODO: listTagsInfoFailureEvent
      throw e;
    }
    return dispatcher.listTagsInfo(metalake);
  }

  @Override
  public Tag getTag(String metalake, String name) throws NoSuchTagException {
    // TODO: getTagPreEvent
    try {
      // TODO: getTagEvent
    } catch (NoSuchTagException e) {
      // TODO: getTagFailureEvent
      throw e;
    }
    return dispatcher.getTag(metalake, name);
  }

  @Override
  public Tag createTag(
      String metalake, String name, String comment, Map<String, String> properties) {
    // TODO: createTagPreEvent
    try {
      // TODO: createTagEvent
    } catch (Exception e) {
      // TODO: createTagFailureEvent
      throw e;
    }
    return dispatcher.createTag(metalake, name, comment, properties);
  }

  @Override
  public Tag alterTag(String metalake, String name, TagChange... changes) {
    // TODO: alterTagPreEvent
    try {
      // TODO: alterTagEvent
    } catch (Exception e) {
      // TODO: alterTagFailureEvent
      throw e;
    }
    return dispatcher.alterTag(metalake, name, changes);
  }

  @Override
  public boolean deleteTag(String metalake, String name) {
    // TODO: deleteTagPreEvent
    try {
      // TODO: deleteTagEvent
    } catch (Exception e) {
      // TODO: deleteTagFailureEvent
      throw e;
    }
    return dispatcher.deleteTag(metalake, name);
  }

  @Override
  public MetadataObject[] listMetadataObjectsForTag(String metalake, String name) {
    // TODO: listMetadataObjectsForTagPreEvent
    try {
      // TODO: listMetadataObjectsForTagEvent
    } catch (Exception e) {
      // TODO: listMetadataObjectsForTagFailureEvent
      throw e;
    }
    return dispatcher.listMetadataObjectsForTag(metalake, name);
  }

  @Override
  public String[] listTagsForMetadataObject(String metalake, MetadataObject metadataObject) {
    // TODO: listTagsForMetadataObjectPreEvent
    try {
      // TODO: listTagsForMetadataObjectEvent
    } catch (Exception e) {
      // TODO: listTagsForMetadataObjectFailureEvent
      throw e;
    }
    return dispatcher.listTagsForMetadataObject(metalake, metadataObject);
  }

  @Override
  public Tag[] listTagsInfoForMetadataObject(String metalake, MetadataObject metadataObject) {
    // TODO: listTagsInfoForMetadataObjectPreEvent
    try {
      // TODO: listTagsInfoForMetadataObjectEvent
    } catch (Exception e) {
      // TODO: listTagsInfoForMetadataObjectFailureEvent
      throw e;
    }
    return dispatcher.listTagsInfoForMetadataObject(metalake, metadataObject);
  }

  @Override
  public String[] associateTagsForMetadataObject(
      String metalake, MetadataObject metadataObject, String[] tagsToAdd, String[] tagsToRemove) {
    // TODO: associateTagsForMetadataObjectPreEvent
    try {
      // TODO: associateTagsForMetadataObjectEvent
    } catch (Exception e) {
      // TODO: associateTagsForMetadataObjectFailureEvent
      throw e;
    }
    return dispatcher.associateTagsForMetadataObject(metalake, metadataObject, tagsToAdd, tagsToRemove);
  }

  @Override
  public Tag getTagForMetadataObject(String metalake, MetadataObject metadataObject, String name) {
    // TODO: getTagForMetadataObjectPreEvent
    try {
      // TODO: getTagForMetadataObjectEvent
    } catch (Exception e) {
      // TODO: getTagForMetadataObjectFailureEvent
      throw e;
    }
    return dispatcher.getTagForMetadataObject(metalake, metadataObject, name);
  }
}
