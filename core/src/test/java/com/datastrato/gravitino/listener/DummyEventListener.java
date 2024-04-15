/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener;

import com.datastrato.gravitino.listener.api.EventListenerPlugin;
import com.datastrato.gravitino.listener.api.event.Event;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.Getter;
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
      Instant waitTime = Instant.now().plusSeconds(20);
      while (getEvents().size() == 0 && Instant.now().isBefore(waitTime)) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          break;
        }
      }
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
