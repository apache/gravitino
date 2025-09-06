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

package org.apache.gravitino.connector;

import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.types.Type;

/** An abstract class representing a base column in a relational database. */
@Evolving
@ToString
@EqualsAndHashCode
public abstract class BaseColumn implements Column {

  protected String name;

  @Nullable protected String comment;

  protected Type dataType;

  protected boolean nullable;

  protected boolean autoIncrement;

  protected Expression defaultValue;

  protected AuditInfo auditInfo;

  /**
   * Returns the name of the column.
   *
   * @return The name of the column.
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * Returns the data type of the column.
   *
   * @return The data type of the column.
   */
  @Override
  public Type dataType() {
    return dataType;
  }

  /**
   * Returns the comment or description for the column.
   *
   * @return The comment or description for the column.
   */
  @Nullable
  @Override
  public String comment() {
    return comment;
  }

  /** Returns true if the column value can be null. */
  @Override
  public boolean nullable() {
    return nullable;
  }

  /** @return True if this column is an auto-increment column. Default is false. */
  @Override
  public boolean autoIncrement() {
    return autoIncrement;
  }

  /**
   * @return The default value of this column, {@link Column#DEFAULT_VALUE_NOT_SET} if not specified
   */
  @Override
  public Expression defaultValue() {
    return defaultValue;
  }

  /** @return The audit information of this column. */
  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  /**
   * Builder interface for creating instances of {@link BaseColumn}.
   *
   * @param <SELF> The type of the builder.
   * @param <T> The type of the column being built.
   */
  interface Builder<SELF extends BaseColumn.Builder<SELF, T>, T extends BaseColumn> {

    SELF withName(String name);

    SELF withComment(String comment);

    SELF withType(Type dataType);

    SELF withNullable(boolean nullable);

    SELF withAutoIncrement(boolean autoIncrement);

    SELF withDefaultValue(Expression defaultValue);

    SELF withAuditInfo(AuditInfo auditInfo);

    T build();
  }

  /**
   * An abstract class implementing the builder interface for {@link BaseColumn}.
   *
   * @param <SELF> The type of the builder.
   * @param <T> The type of the column being built.
   */
  public abstract static class BaseColumnBuilder<
          SELF extends BaseColumn.Builder<SELF, T>, T extends BaseColumn>
      implements BaseColumn.Builder<SELF, T> {
    protected String name;
    protected String comment;
    protected Type dataType;
    protected boolean nullable = true;
    protected boolean autoIncrement = false;
    protected Expression defaultValue;
    protected AuditInfo auditInfo;

    /**
     * Sets the name of the column.
     *
     * @param name The name of the column.
     * @return The builder instance.
     */
    @Override
    public SELF withName(String name) {
      this.name = name;
      return self();
    }

    /**
     * Sets the comment of the column.
     *
     * @param comment The comment or description for the column.
     * @return The builder instance.
     */
    @Override
    public SELF withComment(String comment) {
      this.comment = comment;
      return self();
    }

    /**
     * Sets the data type of the column.
     *
     * @param dataType The data type of the column.
     * @return The builder instance.
     */
    @Override
    public SELF withType(Type dataType) {
      this.dataType = dataType;
      return self();
    }

    /**
     * Sets nullable of the column value.
     *
     * @param nullable Nullable of the column.
     * @return The builder instance.
     */
    @Override
    public SELF withNullable(boolean nullable) {
      this.nullable = nullable;
      return self();
    }

    /**
     * Sets whether the column is an auto-increment column.
     *
     * @param autoIncrement Whether the column is an auto-increment column.
     * @return The builder instance.
     */
    @Override
    public SELF withAutoIncrement(boolean autoIncrement) {
      this.autoIncrement = autoIncrement;
      return self();
    }

    /**
     * Sets the default value of the column.
     *
     * @param defaultValue The default value of the column.
     * @return The builder instance.
     */
    @Override
    public SELF withDefaultValue(Expression defaultValue) {
      this.defaultValue = defaultValue;
      return self();
    }

    /**
     * Sets the audit information of the column.
     *
     * @param auditInfo The audit information of the column.
     * @return The builder instance.
     */
    @Override
    public SELF withAuditInfo(AuditInfo auditInfo) {
      this.auditInfo = auditInfo;
      return self();
    }

    /**
     * Builds the instance of the column with the provided attributes.
     *
     * @return The built column instance.
     */
    @Override
    public T build() {
      // Set default auditInfo if not provided
      if (auditInfo == null) {
        auditInfo = AuditInfo.EMPTY;
      }
      T t = internalBuild();
      return t;
    }

    private SELF self() {
      return (SELF) this;
    }

    /**
     * Builds the concrete instance of the column with the provided attributes.
     *
     * @return The built column instance.
     */
    @Evolving
    protected abstract T internalBuild();
  }
}
