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
import org.apache.gravitino.listener.api.event.AlterTagPreEvent;
import org.apache.gravitino.listener.api.event.AssociateTagsForMetadataObjectPreEvent;
import org.apache.gravitino.listener.api.event.CreateTagPreEvent;
import org.apache.gravitino.listener.api.event.DeleteTagPreEvent;
import org.apache.gravitino.listener.api.event.GetTagForMetadataObjectPreEvent;
import org.apache.gravitino.listener.api.event.GetTagPreEvent;
import org.apache.gravitino.listener.api.event.ListMetadataObjectsForTagPreEvent;
import org.apache.gravitino.listener.api.event.ListTagsForMetadataObjectPreEvent;
import org.apache.gravitino.listener.api.event.ListTagsInfoForMetadataObjectPreEvent;
import org.apache.gravitino.listener.api.event.ListTagsInfoPreEvent;
import org.apache.gravitino.listener.api.event.ListTagsPreEvent;
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
    eventBus.dispatchEvent(new ListTagsPreEvent(PrincipalUtils.getCurrentUserName(), metalake));
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
    eventBus.dispatchEvent(new ListTagsInfoPreEvent(PrincipalUtils.getCurrentUserName(), metalake));
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
    eventBus.dispatchEvent(new GetTagPreEvent(PrincipalUtils.getCurrentUserName(), metalake, name));
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
    TagInfo createTagRequest = new TagInfo(name, comment, properties);

    eventBus.dispatchEvent(
        new CreateTagPreEvent(PrincipalUtils.getCurrentUserName(), metalake, createTagRequest));
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
    AlterTagPreEvent preEvent =
        new AlterTagPreEvent(PrincipalUtils.getCurrentUserName(), metalake, name, changes);

    eventBus.dispatchEvent(preEvent);
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
    DeleteTagPreEvent preEvent =
        new DeleteTagPreEvent(PrincipalUtils.getCurrentUserName(), metalake, name);

    eventBus.dispatchEvent(preEvent);
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
    eventBus.dispatchEvent(
        new ListMetadataObjectsForTagPreEvent(PrincipalUtils.getCurrentUserName(), metalake, name));
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
    eventBus.dispatchEvent(
        new ListTagsForMetadataObjectPreEvent(
            PrincipalUtils.getCurrentUserName(), metalake, metadataObject));

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
    eventBus.dispatchEvent(
        new ListTagsInfoForMetadataObjectPreEvent(
            PrincipalUtils.getCurrentUserName(), metalake, metadataObject));
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
    eventBus.dispatchEvent(
        new AssociateTagsForMetadataObjectPreEvent(
            PrincipalUtils.getCurrentUserName(),
            metalake,
            metadataObject,
            tagsToAdd,
            tagsToRemove));

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
    eventBus.dispatchEvent(
        new GetTagForMetadataObjectPreEvent(
            PrincipalUtils.getCurrentUserName(), metalake, metadataObject, name));
    try {
      // TODO: getTagForMetadataObjectEvent
      return dispatcher.getTagForMetadataObject(metalake, metadataObject, name);
    } catch (Exception e) {
      // TODO: getTagForMetadataObjectFailureEvent
      throw e;
    }
  }
}
