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

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.BaseEvent;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.PreEvent;
import org.apache.gravitino.listener.api.event.SupportsChangingPreEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper for user provided event listener, could contain common logic like exception handling,
 * recording metrics, recording slow event process.
 */
public class EventListenerPluginWrapper implements EventListenerPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(EventListenerPluginWrapper.class);
  private String listenerName;
  private EventListenerPlugin userEventListener;

  public EventListenerPluginWrapper(String listenerName, EventListenerPlugin userEventListener) {
    this.listenerName = listenerName;
    this.userEventListener = userEventListener;
  }

  @Override
  public void init(Map<String, String> properties) {
    throw new RuntimeException(
        "Should not reach here, the event listener has already been initialized.");
  }

  @Override
  public void start() {
    userEventListener.start();
    LOG.info("Start event listener {}.", listenerName);
  }

  @Override
  public void stop() {
    try {
      userEventListener.stop();
      LOG.info("Stop event listener {}.", listenerName);
    } catch (Exception e) {
      LOG.warn("Failed to stop event listener {}.", listenerName, e);
    }
  }

  @Override
  public void onPostEvent(Event event) {
    try {
      userEventListener.onPostEvent(event);
    } catch (Exception e) {
      printExceptionInEventProcess(listenerName, event, e);
    }
  }

  @Override
  public void onPreEvent(PreEvent preEvent) {
    try {
      userEventListener.onPreEvent(preEvent);
    } catch (ForbiddenException e) {
      if (Mode.SYNC.equals(mode())) {
        LOG.warn(
            "Event listener {} process pre event {} throws ForbiddenException, will skip the "
                + "operation.",
            listenerName,
            preEvent.getClass().getSimpleName(),
            e);
        throw e;
      }
      printExceptionInEventProcess(listenerName, preEvent, e);
    } catch (Exception e) {
      printExceptionInEventProcess(listenerName, preEvent, e);
    }
  }

  @Override
  public SupportsChangingPreEvent transformPreEvent(SupportsChangingPreEvent preEvent) {
    return userEventListener.transformPreEvent(preEvent);
  }

  public String listenerName() {
    return listenerName;
  }

  @VisibleForTesting
  public EventListenerPlugin getUserEventListener() {
    return userEventListener;
  }

  private void printExceptionInEventProcess(String listenerName, BaseEvent baseEvent, Exception e) {
    LOG.warn(
        "Event listener {} process event {} failed,",
        listenerName,
        baseEvent.getClass().getSimpleName(),
        e);
  }
}
