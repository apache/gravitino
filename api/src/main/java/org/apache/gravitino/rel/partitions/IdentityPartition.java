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
package org.apache.gravitino.rel.partitions;

import org.apache.gravitino.annotation.Evolving;
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
@Evolving
public interface IdentityPartition extends Partition {

  /**
   * @return The field names of the identity partition.
   */
  String[][] fieldNames();

  /**
   * @return The values of the identity partition. The values are in the same order as the field
   *     names.
   */
  Literal<?>[] values();
}
