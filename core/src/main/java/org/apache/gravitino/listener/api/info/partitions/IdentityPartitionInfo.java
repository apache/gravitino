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

package org.apache.gravitino.listener.api.info.partitions;

import java.util.Map;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.rel.expressions.literals.Literal;

/**
 * An identity partition represents a result of identity partitioning. For example, for Hive
 * partition
 *
 * <pre>`PARTITION (dt='2008-08-08',country='us')`</pre>
 *
 * its partition name is "dt=2008-08-08/country=us", field names are [["dt"], ["country"]] and
 * values are ["2008-08-08", "us"].
 */
@DeveloperApi
public final class IdentityPartitionInfo extends PartitionInfo {
  private final String[][] fieldNames;
  private final Literal<?>[] values;

  /**
   * Constructs IdentityPartitionInfo with specified details.
   *
   * @param name The name of the Partition.
   * @param fieldNames The field names of the identity partition.
   * @param values The values of the identity partition.
   * @param properties A map of properties associated with the Partition.
   */
  public IdentityPartitionInfo(
      String name, String[][] fieldNames, Literal<?>[] values, Map<String, String> properties) {
    super(name, properties);
    this.fieldNames = fieldNames;
    this.values = values;
  }

  /**
   * The field names of the identity partition.
   *
   * @return The field names of the identity partition.
   */
  public String[][] fieldNames() {
    return fieldNames;
  }

  /**
   * The values of the identity partition.
   *
   * @return The values of the identity partition.
   */
  public Literal<?>[] values() {
    return values;
  }
}
