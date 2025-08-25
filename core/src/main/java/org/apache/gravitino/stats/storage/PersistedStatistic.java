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
package org.apache.gravitino.stats.storage;

import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.stats.StatisticValue;

/** Represents a persisted statistic with its name, value, and associated audit information. */
public class PersistedStatistic {

  private final String name;
  private final StatisticValue<?> value;
  private final AuditInfo auditInfo;

  /**
   * Creates an instance of {@link PersistedStatistic}.
   *
   * @param name the name of the statistic
   * @param value the value of the statistic
   * @param auditInfo the audit information associated with the statistic
   * @return a new instance of {@link PersistedStatistic}
   */
  public static PersistedStatistic of(String name, StatisticValue<?> value, AuditInfo auditInfo) {
    return new PersistedStatistic(name, value, auditInfo);
  }

  /**
   * Private constructor for {@link PersistedStatistic}.
   *
   * @param name the name of the statistic
   * @param value the value of the statistic
   * @param auditInfo the audit information associated with the statistic
   */
  private PersistedStatistic(String name, StatisticValue<?> value, AuditInfo auditInfo) {
    this.name = name;
    this.value = value;
    this.auditInfo = auditInfo;
  }

  /**
   * Returns the name of the statistic.
   *
   * @return the name of the statistic
   */
  public String name() {
    return name;
  }

  /**
   * Returns the value of the statistic.
   *
   * @return the value of the statistic
   */
  public StatisticValue<?> value() {
    return value;
  }

  /**
   * Returns the audit information associated with the statistic.
   *
   * @return the audit information
   */
  public AuditInfo auditInfo() {
    return auditInfo;
  }
}
