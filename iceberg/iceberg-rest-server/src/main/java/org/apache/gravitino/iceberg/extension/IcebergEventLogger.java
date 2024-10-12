/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.iceberg.extension;

import java.util.Map;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.PreEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergEventLogger implements EventListenerPlugin {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergEventLogger.class);

  @Override
  public void init(Map<String, String> properties) throws RuntimeException {}

  @Override
  public void start() throws RuntimeException {}

  @Override
  public void stop() throws RuntimeException {}

  @Override
  public void onPostEvent(Event event) {
    LOG.info(
        "Process post event {}, user: {}, identifier: {}",
        event.getClass().getSimpleName(),
        event.user(),
        event.identifier());
  }

  @Override
  public void onPreEvent(PreEvent event) {
    LOG.info(
        "Process pre event {}, user: {}, identifier: {}",
        event.getClass().getSimpleName(),
        event.user(),
        event.identifier());
  }

  @Override
  public Mode mode() {
    return Mode.SYNC;
  }
}
