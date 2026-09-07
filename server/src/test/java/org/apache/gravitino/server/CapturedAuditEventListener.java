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
package org.apache.gravitino.server;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.PreEvent;

/**
 * Captures post-events in a static list so {@link TestGravitinoServer} can assert on audit events
 * dispatched by the in-process server.
 */
public class CapturedAuditEventListener implements EventListenerPlugin {

  private static final List<Event> capturedEvents = new CopyOnWriteArrayList<>();

  /** Clears previously captured events. */
  public static void clear() {
    capturedEvents.clear();
  }

  /**
   * Returns an unmodifiable view of captured post-events.
   *
   * @return captured events
   */
  public static List<Event> getEvents() {
    return Collections.unmodifiableList(capturedEvents);
  }

  @Override
  public void init(Map<String, String> properties) {}

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public void onPostEvent(Event event) {
    capturedEvents.add(event);
  }

  @Override
  public void onPreEvent(PreEvent preEvent) {}

  @Override
  public Mode mode() {
    return Mode.SYNC;
  }
}
