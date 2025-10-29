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

package org.apache.gravitino.lineage.sink;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.openlineage.server.OpenLineage.RunEvent;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.lineage.LineageConfig;
import org.apache.gravitino.listener.api.EventListenerPlugin;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.EventWrapper;
import org.apache.gravitino.utils.ClassUtils;

public class LineageSinkEventListener implements EventListenerPlugin {

  private LineageSink lineageSink;

  @Override
  public void init(Map<String, String> properties) throws RuntimeException {
    String sinkClassName = properties.get(LineageConfig.LINEAGE_SINK_CLASS_NAME);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(sinkClassName),
        LineageConfig.LINEAGE_SINK_CLASS_NAME + " is not set.");
    this.lineageSink = ClassUtils.loadAndGetInstance(sinkClassName);
    lineageSink.initialize(properties);
  }

  @Override
  public void start() throws RuntimeException {}

  @Override
  public void stop() throws RuntimeException {
    if (lineageSink != null) {
      lineageSink.close();
    }
  }

  @Override
  public Mode mode() {
    return Mode.ASYNC_ISOLATED;
  }

  @Override
  public void onPostEvent(Event postEvent) throws RuntimeException {
    EventWrapper<RunEvent> wrapper = (EventWrapper<RunEvent>) postEvent;
    lineageSink.sink(wrapper.getObject());
  }

  @VisibleForTesting
  LineageSink lineageSink() {
    return lineageSink;
  }
}
