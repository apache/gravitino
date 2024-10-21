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
import org.apache.gravitino.Metalake;
import org.apache.gravitino.MetalakeChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.MetalakeAlreadyExistsException;
import org.apache.gravitino.exceptions.MetalakeInUseException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NonEmptyEntityException;
import org.apache.gravitino.listener.api.event.AlterMetalakeEvent;
import org.apache.gravitino.listener.api.event.AlterMetalakeFailureEvent;
import org.apache.gravitino.listener.api.event.CreateMetalakeEvent;
import org.apache.gravitino.listener.api.event.CreateMetalakeFailureEvent;
import org.apache.gravitino.listener.api.event.DropMetalakeEvent;
import org.apache.gravitino.listener.api.event.DropMetalakeFailureEvent;
import org.apache.gravitino.listener.api.event.ListMetalakeEvent;
import org.apache.gravitino.listener.api.event.ListMetalakeFailureEvent;
import org.apache.gravitino.listener.api.event.LoadMetalakeEvent;
import org.apache.gravitino.listener.api.event.LoadMetalakeFailureEvent;
import org.apache.gravitino.listener.api.info.MetalakeInfo;
import org.apache.gravitino.metalake.MetalakeDispatcher;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * {@code MetalakeEventDispatcher} is a decorator for {@link MetalakeDispatcher} that not only
 * delegates metalake operations to the underlying metalake dispatcher but also dispatches
 * corresponding events to an {@link org.apache.gravitino.listener.EventBus} after each operation is
 * completed. This allows for event-driven workflows or monitoring of metalake operations.
 */
public class MetalakeEventDispatcher implements MetalakeDispatcher {
  private final EventBus eventBus;
  private final MetalakeDispatcher dispatcher;

  /**
   * Constructs a MetalakeEventDispatcher with a specified EventBus and MetalakeDispatcher.
   *
   * @param eventBus The EventBus to which events will be dispatched.
   * @param dispatcher The underlying {@link MetalakeDispatcher} that will perform the actual
   *     metalake operations.
   */
  public MetalakeEventDispatcher(EventBus eventBus, MetalakeDispatcher dispatcher) {
    this.eventBus = eventBus;
    this.dispatcher = dispatcher;
  }

  @Override
  public Metalake[] listMetalakes() {
    try {
      Metalake[] metalakes = dispatcher.listMetalakes();
      eventBus.dispatchEvent(new ListMetalakeEvent(PrincipalUtils.getCurrentUserName()));
      return metalakes;
    } catch (Exception e) {
      eventBus.dispatchEvent(new ListMetalakeFailureEvent(PrincipalUtils.getCurrentUserName(), e));
      throw e;
    }
  }

  @Override
  public Metalake loadMetalake(NameIdentifier ident) throws NoSuchMetalakeException {
    try {
      Metalake metalake = dispatcher.loadMetalake(ident);
      eventBus.dispatchEvent(
          new LoadMetalakeEvent(
              PrincipalUtils.getCurrentUserName(), ident, new MetalakeInfo(metalake)));
      return metalake;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new LoadMetalakeFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e));
      throw e;
    }
  }

  @Override
  public boolean metalakeExists(NameIdentifier ident) {
    return dispatcher.metalakeExists(ident);
  }

  @Override
  public Metalake createMetalake(
      NameIdentifier ident, String comment, Map<String, String> properties)
      throws MetalakeAlreadyExistsException {
    try {
      Metalake metalake = dispatcher.createMetalake(ident, comment, properties);
      eventBus.dispatchEvent(
          new CreateMetalakeEvent(
              PrincipalUtils.getCurrentUserName(), ident, new MetalakeInfo(metalake)));
      return metalake;
    } catch (Exception e) {
      MetalakeInfo metalakeInfo = new MetalakeInfo(ident.name(), comment, properties, null);
      eventBus.dispatchEvent(
          new CreateMetalakeFailureEvent(
              PrincipalUtils.getCurrentUserName(), ident, e, metalakeInfo));
      throw e;
    }
  }

  @Override
  public Metalake alterMetalake(NameIdentifier ident, MetalakeChange... changes)
      throws NoSuchMetalakeException, IllegalArgumentException {
    try {
      Metalake metalake = dispatcher.alterMetalake(ident, changes);
      eventBus.dispatchEvent(
          new AlterMetalakeEvent(
              PrincipalUtils.getCurrentUserName(), ident, changes, new MetalakeInfo(metalake)));
      return metalake;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new AlterMetalakeFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e, changes));
      throw e;
    }
  }

  @Override
  public boolean dropMetalake(NameIdentifier ident, boolean force)
      throws NonEmptyEntityException, MetalakeInUseException {
    try {
      boolean isExists = dispatcher.dropMetalake(ident, force);
      eventBus.dispatchEvent(
          new DropMetalakeEvent(PrincipalUtils.getCurrentUserName(), ident, isExists));
      return isExists;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new DropMetalakeFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e));
      throw e;
    }
  }

  @Override
  public void enableMetalake(NameIdentifier ident) throws NoSuchMetalakeException {
    // todo: support enable metalake event
    dispatcher.enableMetalake(ident);
  }

  @Override
  public void disableMetalake(NameIdentifier ident) throws NoSuchMetalakeException {
    // todo: support disable metalake event
    dispatcher.disableMetalake(ident);
  }
}
