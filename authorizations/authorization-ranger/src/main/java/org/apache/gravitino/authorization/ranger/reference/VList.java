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
package org.apache.gravitino.authorization.ranger.reference;

import java.util.List;
import lombok.Setter;

// apache/ranger/security-admin/src/main/java/org/apache/ranger/view/VList.java
public abstract class VList implements java.io.Serializable {
  private static final long serialVersionUID = 1L;

  /** Start index for the result. */
  @Setter protected int startIndex;
  /** Page size used for the result. */
  @Setter protected int pageSize;
  /** Total records in the database for the given search conditions. */
  @Setter protected long totalCount;
  /** Number of rows returned for the search condition. */
  @Setter protected int resultSize;
  /** Sort type. Either desc or asc. */
  @Setter protected String sortType;
  /** Comma separated list of the fields for sorting. */
  @Setter protected String sortBy;

  protected long queryTimeMS = System.currentTimeMillis();

  /** Default constructor. This will set all the attributes to default value. */
  protected VList() {}

  public abstract int getListSize();

  public abstract List<?> getList();

  public int getPageSize() {
    return pageSize;
  }

  @Override
  public String toString() {
    return "VList [startIndex="
        + startIndex
        + ", pageSize="
        + pageSize
        + ", totalCount="
        + totalCount
        + ", resultSize="
        + resultSize
        + ", sortType="
        + sortType
        + ", sortBy="
        + sortBy
        + ", queryTimeMS="
        + queryTimeMS
        + "]";
  }
}
