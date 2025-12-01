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
 * A list partition represents a result of list partitioning. For example, for list partition
 *
 * <pre>
 *     `PARTITION p202204_California VALUES IN (
 *       ("2022-04-01", "Los Angeles"),
 *       ("2022-04-01", "San Francisco")
 *     )`
 *     </pre>
 *
 * its name is "p202204_California" and lists are [["2022-04-01","Los Angeles"], ["2022-04-01", "San
 * Francisco"]].
 */
@Evolving
public interface ListPartition extends Partition {

  /**
   * @return The values of the list partition.
   */
  Literal<?>[][] lists();
}
