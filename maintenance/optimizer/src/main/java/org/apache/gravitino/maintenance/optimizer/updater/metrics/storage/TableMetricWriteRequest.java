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

package org.apache.gravitino.maintenance.optimizer.updater.metrics.storage;

import com.google.common.base.Preconditions;
import java.util.Optional;
import org.apache.gravitino.NameIdentifier;

/** Write request for one table metric record. */
public class TableMetricWriteRequest {
  private final NameIdentifier nameIdentifier;
  private final String metricName;
  private final Optional<String> partition;
  private final MetricRecord metric;

  public TableMetricWriteRequest(
      NameIdentifier nameIdentifier,
      String metricName,
      Optional<String> partition,
      MetricRecord metric) {
    Preconditions.checkArgument(nameIdentifier != null, "nameIdentifier must not be null");
    Preconditions.checkArgument(partition != null, "partition must not be null");
    this.nameIdentifier = nameIdentifier;
    this.metricName = metricName;
    this.partition = partition;
    this.metric = metric;
  }

  public NameIdentifier nameIdentifier() {
    return nameIdentifier;
  }

  public String metricName() {
    return metricName;
  }

  public Optional<String> partition() {
    return partition;
  }

  public MetricRecord metric() {
    return metric;
  }
}
