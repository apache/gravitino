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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.Event;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;

public class DummyEventListener implements EventListenerPlugin {
  Map<String, String> properties;
  @Getter LinkedList<Event> events = new LinkedList<>();

  @Override
  public void init(Map<String, String> properties) {
    this.properties = properties;
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public void onPostEvent(Event event) {
    this.events.add(event);
  }

  @Override
  public Mode mode() {
    return Mode.SYNC;
  }

  public Event popEvent() {
    Assertions.assertTrue(events.size() > 0, "No events to pop");
    return events.removeLast();
  }

  public static class DummyAsyncEventListener extends DummyEventListener {
    public List<Event> tryGetEvents() {
      Awaitility.await()
          .atMost(20, TimeUnit.SECONDS)
          .pollInterval(10, TimeUnit.MILLISECONDS)
          .until(() -> getEvents().size() > 0);
      return getEvents();
    }

    @Override
    public Mode mode() {
      return Mode.ASYNC_SHARED;
    }
  }

  public static class DummyAsyncIsolatedEventListener extends DummyAsyncEventListener {
    @Override
    public Mode mode() {
      return Mode.ASYNC_ISOLATED;
    }
  }
}
