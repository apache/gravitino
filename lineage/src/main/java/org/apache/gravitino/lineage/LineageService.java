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

package org.apache.gravitino.lineage;

import com.google.common.collect.ImmutableSet;
import io.openlineage.server.OpenLineage;
import io.openlineage.server.OpenLineage.RunEvent;
import java.util.Set;
import org.apache.gravitino.lineage.processor.LineageProcessor;
import org.apache.gravitino.lineage.sink.LineageSinkManager;
import org.apache.gravitino.lineage.source.LineageSource;
import org.apache.gravitino.server.web.SupportsRESTPackages;
import org.apache.gravitino.utils.ClassUtils;

/**
 * The LineageService manages the life cycle of lineage sinks, sources, and processors. It provides
 * {@code dispatchLineageEvent} method for lineage source to dispatch lineage events to the sinks.
 */
public class LineageService implements LineageDispatcher, SupportsRESTPackages {
  private LineageSinkManager sinkManager;
  private LineageSource source;
  private LineageProcessor processor;

  public void initialize(LineageConfig lineageConfig) {
    String sourceName = lineageConfig.source();
    String sourceClass = lineageConfig.sourceClass();
    this.source = ClassUtils.loadAndGetInstance(sourceClass);
    this.sinkManager = new LineageSinkManager();

    String processorClassName = lineageConfig.processorClass();
    this.processor = ClassUtils.loadAndGetInstance(processorClassName);

    sinkManager.initialize(lineageConfig.sinks(), lineageConfig.getSinkConfigs());
    source.initialize(lineageConfig.getConfigsWithPrefix(sourceName), this);
  }

  @Override
  public void close() {
    if (source != null) {
      source.close();
      source = null;
    }
    if (sinkManager != null) {
      sinkManager.close();
      sinkManager = null;
    }
  }

  @Override
  public boolean dispatchLineageEvent(OpenLineage.RunEvent runEvent) {
    if (runEvent == null) {
      return false;
    }
    if (sinkManager.isHighWatermark()) {
      return false;
    }
    RunEvent newEvent = processor.process(runEvent);
    sinkManager.sink(newEvent);
    return true;
  }

  @Override
  public Set<String> getRESTPackages() {
    if (source instanceof SupportsRESTPackages) {
      return ((SupportsRESTPackages) source).getRESTPackages();
    }
    return ImmutableSet.of();
  }
}
