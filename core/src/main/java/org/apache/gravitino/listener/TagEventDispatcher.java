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

/**
 * {@code TagEventDispatcher} is a decorator for {@link TagDispatcher} that not only delegates tag
 * operations to the underlying tag dispatcher but also dispatches corresponding events to an {@link
 * EventBus} after each operation is completed. This allows for event-driven workflows or monitoring
 * of tag operations.
 */
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
      return dispatcher.listTags(metalake);
    } catch (Exception e) {
      // TODO: listTagFailureEvent
      throw e;
    }
  }

  @Override
  public Tag[] listTagsInfo(String metalake) {
    // TODO: listTagsInfoPreEvent
    try {
      // TODO: listTagsInfoEvent
      return dispatcher.listTagsInfo(metalake);
    } catch (Exception e) {
      // TODO: listTagsInfoFailureEvent
      throw e;
    }
  }

  @Override
  public Tag getTag(String metalake, String name) throws NoSuchTagException {
    // TODO: getTagPreEvent
    try {
      // TODO: getTagEvent
      return dispatcher.getTag(metalake, name);
    } catch (NoSuchTagException e) {
      // TODO: getTagFailureEvent
      throw e;
    }
  }

  @Override
  public Tag createTag(
      String metalake, String name, String comment, Map<String, String> properties) {
    // TODO: createTagPreEvent
    try {
      // TODO: createTagEvent
      return dispatcher.createTag(metalake, name, comment, properties);
    } catch (Exception e) {
      // TODO: createTagFailureEvent
      throw e;
    }
  }

  @Override
  public Tag alterTag(String metalake, String name, TagChange... changes) {
    // TODO: alterTagPreEvent
    try {
      // TODO: alterTagEvent
      return dispatcher.alterTag(metalake, name, changes);
    } catch (Exception e) {
      // TODO: alterTagFailureEvent
      throw e;
    }
  }

  @Override
  public boolean deleteTag(String metalake, String name) {
    // TODO: deleteTagPreEvent
    try {
      // TODO: deleteTagEvent
      return dispatcher.deleteTag(metalake, name);
    } catch (Exception e) {
      // TODO: deleteTagFailureEvent
      throw e;
    }
  }

  @Override
  public MetadataObject[] listMetadataObjectsForTag(String metalake, String name) {
    // TODO: listMetadataObjectsForTagPreEvent
    try {
      // TODO: listMetadataObjectsForTagEvent
      return dispatcher.listMetadataObjectsForTag(metalake, name);
    } catch (Exception e) {
      // TODO: listMetadataObjectsForTagFailureEvent
      throw e;
    }
  }

  @Override
  public String[] listTagsForMetadataObject(String metalake, MetadataObject metadataObject) {
    // TODO: listTagsForMetadataObjectPreEvent
    try {
      // TODO: listTagsForMetadataObjectEvent
      return dispatcher.listTagsForMetadataObject(metalake, metadataObject);
    } catch (Exception e) {
      // TODO: listTagsForMetadataObjectFailureEvent
      throw e;
    }
  }

  @Override
  public Tag[] listTagsInfoForMetadataObject(String metalake, MetadataObject metadataObject) {
    // TODO: listTagsInfoForMetadataObjectPreEvent
    try {
      // TODO: listTagsInfoForMetadataObjectEvent
      return dispatcher.listTagsInfoForMetadataObject(metalake, metadataObject);
    } catch (Exception e) {
      // TODO: listTagsInfoForMetadataObjectFailureEvent
      throw e;
    }
  }

  @Override
  public String[] associateTagsForMetadataObject(
      String metalake, MetadataObject metadataObject, String[] tagsToAdd, String[] tagsToRemove) {
    // TODO: associateTagsForMetadataObjectPreEvent
    try {
      // TODO: associateTagsForMetadataObjectEvent
      return dispatcher.associateTagsForMetadataObject(
              metalake, metadataObject, tagsToAdd, tagsToRemove);
    } catch (Exception e) {
      // TODO: associateTagsForMetadataObjectFailureEvent
      throw e;
    }
  }

  @Override
  public Tag getTagForMetadataObject(String metalake, MetadataObject metadataObject, String name) {
    // TODO: getTagForMetadataObjectPreEvent
    try {
      // TODO: getTagForMetadataObjectEvent
      return dispatcher.getTagForMetadataObject(metalake, metadataObject, name);
    } catch (Exception e) {
      // TODO: getTagForMetadataObjectFailureEvent
      throw e;
    }
  }
}
