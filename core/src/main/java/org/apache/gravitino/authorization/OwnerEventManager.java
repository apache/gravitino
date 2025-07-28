/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.authorization;

import com.google.common.annotations.VisibleForTesting;
import java.util.Optional;
import lombok.Getter;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.listener.EventBus;
import org.apache.gravitino.listener.api.event.GetOwnerEvent;
import org.apache.gravitino.listener.api.event.GetOwnerFailureEvent;
import org.apache.gravitino.listener.api.event.GetOwnerPreEvent;
import org.apache.gravitino.listener.api.event.SetOwnerEvent;
import org.apache.gravitino.listener.api.event.SetOwnerFailureEvent;
import org.apache.gravitino.listener.api.event.SetOwnerPreEvent;
import org.apache.gravitino.listener.api.info.OwnerInfo;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.PrincipalUtils;

public class OwnerEventManager implements OwnerDispatcher {
  private final EventBus eventBus;

  @Getter private OwnerDispatcher ownerDispatcher;

  public OwnerEventManager(EventBus eventBus, OwnerDispatcher ownerDispatcher) {
    this.eventBus = eventBus;
    this.ownerDispatcher = ownerDispatcher;
  }

  @VisibleForTesting
  public void setOwnerManager(OwnerDispatcher ownerDispatcher) {
    this.ownerDispatcher = ownerDispatcher;
  }

  @Override
  public void setOwner(
      String metalake, MetadataObject metadataObject, String ownerName, Owner.Type ownerType) {
    NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
    String user = PrincipalUtils.getCurrentUserName();
    OwnerInfo ownerInfo = new OwnerInfo(ownerName, ownerType);
    MetadataObject.Type type = metadataObject.type();
    eventBus.dispatchEvent(new SetOwnerPreEvent(user, identifier, ownerInfo, type));
    try {
      ownerDispatcher.setOwner(metalake, metadataObject, ownerName, ownerType);
      eventBus.dispatchEvent(new SetOwnerEvent(user, identifier, ownerInfo, type));
    } catch (Exception e) {
      eventBus.dispatchEvent(new SetOwnerFailureEvent(user, identifier, e, ownerInfo, type));
      throw e;
    }
  }

  @Override
  public Optional<Owner> getOwner(String metalake, MetadataObject metadataObject) {
    NameIdentifier identifier = MetadataObjectUtil.toEntityIdent(metalake, metadataObject);
    String user = PrincipalUtils.getCurrentUserName();
    OwnerInfo ownerInfo = null;
    MetadataObject.Type type = metadataObject.type();
    eventBus.dispatchEvent(new GetOwnerPreEvent(user, identifier, ownerInfo, type));
    try {
      Optional<Owner> owner = ownerDispatcher.getOwner(metalake, metadataObject);
      if (owner.isPresent()) {
        ownerInfo = new OwnerInfo(owner.get().name(), owner.get().type());
      }

      eventBus.dispatchEvent(new GetOwnerEvent(user, identifier, ownerInfo, type));
      return owner;
    } catch (Exception e) {
      eventBus.dispatchEvent(new GetOwnerFailureEvent(user, identifier, e, ownerInfo, type));
      throw e;
    }
  }
}
