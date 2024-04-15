/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.FilesetAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchFilesetException;
import com.datastrato.gravitino.exceptions.NoSuchSchemaException;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.file.FilesetChange;
import com.datastrato.gravitino.listener.EventBus;
import com.datastrato.gravitino.listener.api.event.AlterFilesetEvent;
import com.datastrato.gravitino.listener.api.event.AlterFilesetFailureEvent;
import com.datastrato.gravitino.listener.api.event.CreateFilesetEvent;
import com.datastrato.gravitino.listener.api.event.CreateFilesetFailureEvent;
import com.datastrato.gravitino.listener.api.event.DropFilesetEvent;
import com.datastrato.gravitino.listener.api.event.DropFilesetFailureEvent;
import com.datastrato.gravitino.listener.api.event.ListFilesetEvent;
import com.datastrato.gravitino.listener.api.event.ListFilesetFailureEvent;
import com.datastrato.gravitino.listener.api.event.LoadFilesetEvent;
import com.datastrato.gravitino.listener.api.event.LoadFilesetFailureEvent;
import com.datastrato.gravitino.listener.api.info.FilesetInfo;
import com.datastrato.gravitino.utils.PrincipalUtils;
import java.util.Map;

/**
 * {@code FilesetEventDispatcher} is a decorator for {@link FilesetDispatcher} that not only
 * delegates fileset operations to the underlying catalog dispatcher but also dispatches
 * corresponding events to an {@link EventBus} after each operation is completed. This allows for
 * event-driven workflows or monitoring of fileset operations.
 */
public class FilesetEventDispatcher implements FilesetDispatcher {
  private final EventBus eventBus;
  private final FilesetDispatcher dispatcher;

  public FilesetEventDispatcher(EventBus eventBus, FilesetDispatcher dispatcher) {
    this.eventBus = eventBus;
    this.dispatcher = dispatcher;
  }

  @Override
  public NameIdentifier[] listFilesets(Namespace namespace) throws NoSuchSchemaException {
    try {
      NameIdentifier[] nameIdentifiers = dispatcher.listFilesets(namespace);
      eventBus.dispatchEvent(new ListFilesetEvent(PrincipalUtils.getCurrentUserName(), namespace));
      return nameIdentifiers;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new ListFilesetFailureEvent(PrincipalUtils.getCurrentUserName(), namespace, e));
      throw e;
    }
  }

  @Override
  public Fileset loadFileset(NameIdentifier ident) throws NoSuchFilesetException {
    try {
      Fileset fileset = dispatcher.loadFileset(ident);
      eventBus.dispatchEvent(new LoadFilesetEvent(PrincipalUtils.getCurrentUserName(), ident));
      return fileset;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new LoadFilesetFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e));
      throw e;
    }
  }

  @Override
  public Fileset createFileset(
      NameIdentifier ident,
      String comment,
      Fileset.Type type,
      String storageLocation,
      Map<String, String> properties)
      throws NoSuchSchemaException, FilesetAlreadyExistsException {
    try {
      Fileset fileset = dispatcher.createFileset(ident, comment, type, storageLocation, properties);
      eventBus.dispatchEvent(
          new CreateFilesetEvent(
              PrincipalUtils.getCurrentUserName(), ident, new FilesetInfo(fileset)));
      return fileset;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new CreateFilesetFailureEvent(
              PrincipalUtils.getCurrentUserName(),
              ident,
              e,
              new FilesetInfo(ident.name(), comment, type, storageLocation, properties, null)));
      throw e;
    }
  }

  @Override
  public Fileset alterFileset(NameIdentifier ident, FilesetChange... changes)
      throws NoSuchFilesetException, IllegalArgumentException {
    try {
      Fileset fileset = dispatcher.alterFileset(ident, changes);
      eventBus.dispatchEvent(
          new AlterFilesetEvent(
              PrincipalUtils.getCurrentUserName(), ident, changes, new FilesetInfo(fileset)));
      return fileset;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new AlterFilesetFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e, changes));
      throw e;
    }
  }

  @Override
  public boolean dropFileset(NameIdentifier ident) {
    try {
      boolean isExists = dispatcher.dropFileset(ident);
      eventBus.dispatchEvent(
          new DropFilesetEvent(PrincipalUtils.getCurrentUserName(), ident, isExists));
      return isExists;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new DropFilesetFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e));
      throw e;
    }
  }
}
