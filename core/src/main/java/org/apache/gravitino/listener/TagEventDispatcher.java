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
import org.apache.gravitino.listener.api.event.AlterTagEvent;
import org.apache.gravitino.listener.api.event.AssociateTagsForMetadataObjectEvent;
import org.apache.gravitino.listener.api.event.CreateTagEvent;
import org.apache.gravitino.listener.api.event.DeleteTagEvent;
import org.apache.gravitino.listener.api.event.GetTagEvent;
import org.apache.gravitino.listener.api.event.GetTagForMetadataObjectEvent;
import org.apache.gravitino.listener.api.event.ListMetadataObjectsForTagEvent;
import org.apache.gravitino.listener.api.event.ListTagEvent;
import org.apache.gravitino.listener.api.event.ListTagInfoEvent;
import org.apache.gravitino.listener.api.event.ListTagsForMetadataObjectEvent;
import org.apache.gravitino.listener.api.event.ListTagsInfoForMetadataObjectEvent;
import org.apache.gravitino.listener.api.info.TagInfo;
import org.apache.gravitino.tag.Tag;
import org.apache.gravitino.tag.TagChange;
import org.apache.gravitino.tag.TagDispatcher;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * {@code TagEventDispatcher} is a decorator for {@link TagDispatcher} that not only delegates tag
 * operations to the underlying tag dispatcher but also dispatches corresponding events to an {@link
 * EventBus} after each operation is completed. This allows for event-driven workflows or monitoring
 * of tag operations.
 */
public class TagEventDispatcher implements TagDispatcher {
  private final EventBus eventBus;
  private final TagDispatcher dispatcher;

  public TagEventDispatcher(EventBus eventBus, TagDispatcher dispatcher) {
    this.eventBus = eventBus;
    this.dispatcher = dispatcher;
  }

  @Override
  public String[] listTags(String metalake) {
    // TODO: listTagsPreEvent
    try {
      String[] tagNames = dispatcher.listTags(metalake);
      eventBus.dispatchEvent(new ListTagEvent(PrincipalUtils.getCurrentUserName(), metalake));
      return tagNames;
    } catch (Exception e) {
      // TODO: listTagFailureEvent
      throw e;
    }
  }

  @Override
  public Tag[] listTagsInfo(String metalake) {
    // TODO: listTagsInfoPreEvent
    try {
      Tag[] tags = dispatcher.listTagsInfo(metalake);
      eventBus.dispatchEvent(
          new ListTagInfoEvent(PrincipalUtils.getCurrentUserName(), metalake, tags));
      return tags;
    } catch (Exception e) {
      // TODO: listTagsInfoFailureEvent
      throw e;
    }
  }

  @Override
  public Tag getTag(String metalake, String name) throws NoSuchTagException {
    // TODO: getTagPreEvent
    try {
      Tag tag = dispatcher.getTag(metalake, name);
      eventBus.dispatchEvent(
          new GetTagEvent(PrincipalUtils.getCurrentUserName(), metalake, name, tag));
      return tag;
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
      Tag tag = dispatcher.createTag(metalake, name, comment, properties);
      eventBus.dispatchEvent(
          new CreateTagEvent(
              PrincipalUtils.getCurrentUserName(),
              metalake,
              new TagInfo(tag.name(), tag.comment(), tag.properties())));
      return tag;
    } catch (Exception e) {
      // TODO: createTagFailureEvent
      throw e;
    }
  }

  @Override
  public Tag alterTag(String metalake, String name, TagChange... changes) {
    // TODO: alterTagPreEvent
    try {
      Tag tag = dispatcher.alterTag(metalake, name, changes);
      eventBus.dispatchEvent(
          new AlterTagEvent(
              PrincipalUtils.getCurrentUserName(),
              metalake,
              changes,
              new TagInfo(tag.name(), tag.comment(), tag.properties())));
      return tag;
    } catch (Exception e) {
      // TODO: alterTagFailureEvent
      throw e;
    }
  }

  @Override
  public boolean deleteTag(String metalake, String name) {
    // TODO: deleteTagPreEvent
    try {
      boolean isExists = dispatcher.deleteTag(metalake, name);
      eventBus.dispatchEvent(
          new DeleteTagEvent(PrincipalUtils.getCurrentUserName(), metalake, isExists));
      return isExists;
    } catch (Exception e) {
      // TODO: deleteTagFailureEvent
      throw e;
    }
  }

  @Override
  public MetadataObject[] listMetadataObjectsForTag(String metalake, String name) {
    // TODO: listMetadataObjectsForTagPreEvent
    try {
      MetadataObject[] metadataObjects = dispatcher.listMetadataObjectsForTag(metalake, name);
      eventBus.dispatchEvent(
          new ListMetadataObjectsForTagEvent(
              PrincipalUtils.getCurrentUserName(), metalake, name, metadataObjects));
      return metadataObjects;
    } catch (Exception e) {
      // TODO: listMetadataObjectsForTagFailureEvent
      throw e;
    }
  }

  @Override
  public String[] listTagsForMetadataObject(String metalake, MetadataObject metadataObject) {
    // TODO: listTagsForMetadataObjectPreEvent
    try {
      String[] tags = dispatcher.listTagsForMetadataObject(metalake, metadataObject);
      eventBus.dispatchEvent(
          new ListTagsForMetadataObjectEvent(
              PrincipalUtils.getCurrentUserName(), metalake, metadataObject, tags));
      return tags;
    } catch (Exception e) {
      // TODO: listTagsForMetadataObjectFailureEvent
      throw e;
    }
  }

  @Override
  public Tag[] listTagsInfoForMetadataObject(String metalake, MetadataObject metadataObject) {
    // TODO: listTagsInfoForMetadataObjectPreEvent
    try {
      Tag[] tags = dispatcher.listTagsInfoForMetadataObject(metalake, metadataObject);
      eventBus.dispatchEvent(
          new ListTagsInfoForMetadataObjectEvent(
              PrincipalUtils.getCurrentUserName(), metalake, metadataObject, tags));
      return tags;
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
      String[] associatedTags =
          dispatcher.associateTagsForMetadataObject(
              metalake, metadataObject, tagsToAdd, tagsToRemove);
      eventBus.dispatchEvent(
          new AssociateTagsForMetadataObjectEvent(
              PrincipalUtils.getCurrentUserName(),
              metalake,
              metadataObject,
              tagsToAdd,
              tagsToRemove,
              associatedTags));
      return associatedTags;
    } catch (Exception e) {
      // TODO: associateTagsForMetadataObjectFailureEvent
      throw e;
    }
  }

  @Override
  public Tag getTagForMetadataObject(String metalake, MetadataObject metadataObject, String name) {
    // TODO: getTagForMetadataObjectPreEvent
    try {
      Tag tag = dispatcher.getTagForMetadataObject(metalake, metadataObject, name);
      eventBus.dispatchEvent(
          new GetTagForMetadataObjectEvent(
              PrincipalUtils.getCurrentUserName(), metalake, metadataObject, name, tag));
      return tag;
    } catch (Exception e) {
      // TODO: getTagForMetadataObjectFailureEvent
      throw e;
    }
  }
}
