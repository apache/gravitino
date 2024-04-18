/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.metalake;

import com.datastrato.gravitino.Metalake;
import com.datastrato.gravitino.MetalakeChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.exceptions.MetalakeAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.listener.EventBus;
import com.datastrato.gravitino.listener.api.event.AlterMetalakeEvent;
import com.datastrato.gravitino.listener.api.event.AlterMetalakeFailureEvent;
import com.datastrato.gravitino.listener.api.event.CreateMetalakeEvent;
import com.datastrato.gravitino.listener.api.event.CreateMetalakeFailureEvent;
import com.datastrato.gravitino.listener.api.event.DropMetalakeEvent;
import com.datastrato.gravitino.listener.api.event.DropMetalakeFailureEvent;
import com.datastrato.gravitino.listener.api.event.ListMetalakeEvent;
import com.datastrato.gravitino.listener.api.event.ListMetalakeFailureEvent;
import com.datastrato.gravitino.listener.api.event.LoadMetalakeEvent;
import com.datastrato.gravitino.listener.api.event.LoadMetalakeFailureEvent;
import com.datastrato.gravitino.listener.api.info.MetalakeInfo;
import com.datastrato.gravitino.utils.PrincipalUtils;
import java.util.Map;

/**
 * {@code MetalakeEventDispatcher} is a decorator for {@link MetalakeDispatcher} that not only
 * delegates metalake operations to the underlying metalake dispatcher but also dispatches
 * corresponding events to an {@link EventBus} after each operation is completed. This allows for
 * event-driven workflows or monitoring of metalake operations.
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
  public boolean dropMetalake(NameIdentifier ident) {
    try {
      boolean isExists = dispatcher.dropMetalake(ident);
      eventBus.dispatchEvent(
          new DropMetalakeEvent(PrincipalUtils.getCurrentUserName(), ident, isExists));
      return isExists;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new DropMetalakeFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e));
      throw e;
    }
  }
}
