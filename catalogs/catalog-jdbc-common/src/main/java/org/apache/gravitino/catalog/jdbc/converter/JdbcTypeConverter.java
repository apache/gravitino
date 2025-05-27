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
package org.apache.gravitino.catalog.jdbc.converter;

import java.util.Objects;
import org.apache.gravitino.connector.DataTypeConverter;

public abstract class JdbcTypeConverter
    implements DataTypeConverter<String, JdbcTypeConverter.JdbcTypeBean> {

  public static final String DATE = "date";
  public static final String TIME = "time";
  public static final String TIMESTAMP = "timestamp";
  public static final String VARCHAR = "varchar";
  public static final String TEXT = "text";
  public static final int PRECISION_SECOND = 0;

  public static class JdbcTypeBean {
    /** Data type name. */
    private String typeName;

    /** Column size. For example: 20 in varchar (20) and 10 in decimal (10,2). */
    private Integer columnSize;

    /** Scale. For example: 2 in decimal (10,2). */
    private Integer scale;

    private Integer datetimePrecision;

    public JdbcTypeBean(String typeName) {
      this.typeName = typeName;
    }

    public String getTypeName() {
      return typeName;
    }

    public void setTypeName(String typeName) {
      this.typeName = typeName;
    }

    public Integer getColumnSize() {
      return columnSize;
    }

    public void setColumnSize(Integer columnSize) {
      this.columnSize = columnSize;
    }

    public Integer getScale() {
      return scale;
    }

    public void setScale(Integer scale) {
      this.scale = scale;
    }

    public Integer getDatetimePrecision() {
      return datetimePrecision;
    }

    public void setDatetimePrecision(Integer datetimePrecision) {
      this.datetimePrecision = datetimePrecision;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof JdbcTypeBean)) return false;
      JdbcTypeBean typeBean = (JdbcTypeBean) o;
      return Objects.equals(typeName, typeBean.typeName)
          && Objects.equals(columnSize, typeBean.columnSize)
          && Objects.equals(scale, typeBean.scale)
          && Objects.equals(datetimePrecision, typeBean.datetimePrecision);
    }

    @Override
    public int hashCode() {
      return Objects.hash(typeName, columnSize, scale, datetimePrecision);
    }

    @Override
    public String toString() {
      return "JdbcTypeBean{"
          + "typeName='"
          + typeName
          + '\''
          + ", columnSize='"
          + columnSize
          + '\''
          + ", scale='"
          + scale
          + '\''
          + ", datetimePrecision='"
          + datetimePrecision
          + '\''
          + '}';
    }
  }
}
