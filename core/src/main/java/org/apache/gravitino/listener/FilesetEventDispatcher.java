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

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.audit.CallerContext;
import org.apache.gravitino.catalog.FilesetDispatcher;
import org.apache.gravitino.exceptions.FilesetAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.exceptions.NoSuchLocationNameException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.file.FileInfo;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.listener.api.event.AlterFilesetEvent;
import org.apache.gravitino.listener.api.event.AlterFilesetFailureEvent;
import org.apache.gravitino.listener.api.event.AlterFilesetPreEvent;
import org.apache.gravitino.listener.api.event.CreateFilesetEvent;
import org.apache.gravitino.listener.api.event.CreateFilesetFailureEvent;
import org.apache.gravitino.listener.api.event.CreateFilesetPreEvent;
import org.apache.gravitino.listener.api.event.DropFilesetEvent;
import org.apache.gravitino.listener.api.event.DropFilesetFailureEvent;
import org.apache.gravitino.listener.api.event.DropFilesetPreEvent;
import org.apache.gravitino.listener.api.event.GetFileLocationEvent;
import org.apache.gravitino.listener.api.event.GetFileLocationFailureEvent;
import org.apache.gravitino.listener.api.event.GetFileLocationPreEvent;
import org.apache.gravitino.listener.api.event.ListFilesEvent;
import org.apache.gravitino.listener.api.event.ListFilesFailureEvent;
import org.apache.gravitino.listener.api.event.ListFilesPreEvent;
import org.apache.gravitino.listener.api.event.ListFilesetEvent;
import org.apache.gravitino.listener.api.event.ListFilesetFailureEvent;
import org.apache.gravitino.listener.api.event.ListFilesetPreEvent;
import org.apache.gravitino.listener.api.event.LoadFilesetEvent;
import org.apache.gravitino.listener.api.event.LoadFilesetFailureEvent;
import org.apache.gravitino.listener.api.event.LoadFilesetPreEvent;
import org.apache.gravitino.listener.api.info.FilesetInfo;
import org.apache.gravitino.utils.PrincipalUtils;

/**
 * {@code FilesetEventDispatcher} is a decorator for {@link FilesetDispatcher} that not only
 * delegates fileset operations to the underlying catalog dispatcher but also dispatches
 * corresponding events to an {@link org.apache.gravitino.listener.EventBus} after each operation is
 * completed. This allows for event-driven workflows or monitoring of fileset operations.
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
    eventBus.dispatchEvent(new ListFilesetPreEvent(PrincipalUtils.getCurrentUserName(), namespace));
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
  public FileInfo[] listFiles(NameIdentifier ident, String locationName, String subPath)
      throws NoSuchFilesetException, IOException {
    eventBus.dispatchEvent(
        new ListFilesPreEvent(PrincipalUtils.getCurrentUserName(), ident, locationName, subPath));
    try {
      FileInfo[] fileInfos = dispatcher.listFiles(ident, locationName, subPath);
      eventBus.dispatchEvent(
          new ListFilesEvent(PrincipalUtils.getCurrentUserName(), ident, locationName, subPath));
      return fileInfos;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new ListFilesFailureEvent(
              PrincipalUtils.getCurrentUserName(), ident, locationName, subPath, e));
      throw e;
    }
  }

  @Override
  public Fileset loadFileset(NameIdentifier ident) throws NoSuchFilesetException {
    eventBus.dispatchEvent(new LoadFilesetPreEvent(PrincipalUtils.getCurrentUserName(), ident));
    try {
      Fileset fileset = dispatcher.loadFileset(ident);
      eventBus.dispatchEvent(
          new LoadFilesetEvent(
              PrincipalUtils.getCurrentUserName(), ident, new FilesetInfo(fileset)));
      return fileset;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new LoadFilesetFailureEvent(PrincipalUtils.getCurrentUserName(), ident, e));
      throw e;
    }
  }

  @Override
  public Fileset createMultipleLocationFileset(
      NameIdentifier ident,
      String comment,
      Fileset.Type type,
      Map<String, String> storageLocations,
      Map<String, String> properties)
      throws NoSuchSchemaException, FilesetAlreadyExistsException {
    FilesetInfo createFileRequest =
        new FilesetInfo(ident.name(), comment, type, storageLocations, properties, null);
    eventBus.dispatchEvent(
        new CreateFilesetPreEvent(PrincipalUtils.getCurrentUserName(), ident, createFileRequest));
    try {
      Fileset fileset =
          dispatcher.createMultipleLocationFileset(
              ident, comment, type, storageLocations, properties);
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
              new FilesetInfo(ident.name(), comment, type, storageLocations, properties, null)));
      throw e;
    }
  }

  @Override
  public Fileset alterFileset(NameIdentifier ident, FilesetChange... changes)
      throws NoSuchFilesetException, IllegalArgumentException {
    eventBus.dispatchEvent(
        new AlterFilesetPreEvent(PrincipalUtils.getCurrentUserName(), ident, changes));
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
    eventBus.dispatchEvent(new DropFilesetPreEvent(PrincipalUtils.getCurrentUserName(), ident));
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

  @Override
  public String getFileLocation(NameIdentifier ident, String subPath, String locationName)
      throws NoSuchFilesetException, NoSuchLocationNameException {
    eventBus.dispatchEvent(
        new GetFileLocationPreEvent(
            PrincipalUtils.getCurrentUserName(), ident, subPath, locationName));
    try {
      String actualFileLocation = dispatcher.getFileLocation(ident, subPath, locationName);
      // get the audit info from the thread local context
      ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
      CallerContext callerContext = CallerContext.CallerContextHolder.get();
      if (callerContext != null && callerContext.context() != null) {
        builder.putAll(callerContext.context());
      }
      eventBus.dispatchEvent(
          new GetFileLocationEvent(
              PrincipalUtils.getCurrentUserName(),
              ident,
              actualFileLocation,
              subPath,
              locationName,
              builder.build()));
      return actualFileLocation;
    } catch (Exception e) {
      eventBus.dispatchEvent(
          new GetFileLocationFailureEvent(
              PrincipalUtils.getCurrentUserName(), ident, subPath, locationName, e));
      throw e;
    }
  }
}
