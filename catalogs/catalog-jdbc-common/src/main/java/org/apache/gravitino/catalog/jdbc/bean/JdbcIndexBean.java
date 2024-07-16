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
package org.apache.gravitino.catalog.jdbc.bean;

import java.util.Objects;
import org.apache.gravitino.rel.indexes.Index;

/** Store JDBC index information. */
public class JdbcIndexBean {

  private final Index.IndexType indexType;

  private final String colName;

  private final String name;

  /** Used for sorting */
  private final int order;

  public JdbcIndexBean(Index.IndexType indexType, String colName, String name, int order) {
    this.indexType = indexType;
    this.colName = colName;
    this.name = name;
    this.order = order;
  }

  public Index.IndexType getIndexType() {
    return indexType;
  }

  public String getColName() {
    return colName;
  }

  public String getName() {
    return name;
  }

  public int getOrder() {
    return order;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof JdbcIndexBean)) return false;
    JdbcIndexBean that = (JdbcIndexBean) o;
    return order == that.order
        && indexType == that.indexType
        && Objects.equals(colName, that.colName)
        && Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(indexType, colName, name, order);
  }

  @Override
  public String toString() {
    return "JdbcIndexBean{"
        + "indexType="
        + indexType
        + ", colName='"
        + colName
        + '\''
        + ", name='"
        + name
        + '\''
        + ", order="
        + order
        + '}';
  }
}
