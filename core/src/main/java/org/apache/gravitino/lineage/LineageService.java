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

import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import io.openlineage.server.OpenLineage;
import io.openlineage.server.OpenLineage.RunEvent;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import org.apache.gravitino.utils.ClassUtils;

public class LineageService implements LineageDispatcher {
  private LineageSinkManager sinkManager;
  private LineageSource source;
  private LineageProcessor processor;

  public void initialize(LineageConfig lineageConfig) {
    String sourceName = lineageConfig.source();
    this.source = ClassUtils.loadClass(sourceName);
    this.sinkManager = new LineageSinkManager();

    String processorClassName = lineageConfig.processor();
    this.processor = ClassUtils.loadClass(processorClassName);

    sinkManager.initialize(lineageConfig.sinks(), lineageConfig.getLineageConfigMap());
    source.initialize(lineageConfig.getConfigsWithPrefix(sourceName), this);
  }

  public void stop() {
    source.stop();
    sinkManager.stop();
  }

  @Override
  public boolean dispatchLineageEvent(OpenLineage.RunEvent runEvent) {
    if (sinkManager.isHighWaterMark()) {
      return false;
    }

    RunEvent newEvent = processor.process(runEvent);
    sinkManager.sink(newEvent);
    return true;
  }

  private LineageSource loadLineageSource(String source) {
    try {
      Class<? extends LineageSource> clz =
          lookupLineageSource(source, this.getClass().getClassLoader());
      return clz.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Class<? extends LineageSource> lookupLineageSource(String provider, ClassLoader cl) {
    ServiceLoader<LineageSource> loader = ServiceLoader.load(LineageSource.class, cl);
    List<Class<? extends LineageSource>> providers =
        Streams.stream(loader.iterator())
            .filter(p -> p.shortName().equalsIgnoreCase(provider))
            .map(LineageSource::getClass)
            .collect(Collectors.toList());

    if (providers.isEmpty()) {
      throw new IllegalArgumentException("No GravitinoAuxiliaryService found for: " + provider);
    } else if (providers.size() > 1) {
      throw new IllegalArgumentException(
          "Multiple GravitinoAuxiliaryService found for: " + provider);
    } else {
      return Iterables.getOnlyElement(providers);
    }
  }

  private LineageProcessor loadLineageProcessor(String processor) {
    try {
      return (LineageProcessor) Class.forName(processor).getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
