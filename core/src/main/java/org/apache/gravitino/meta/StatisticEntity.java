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
package org.apache.gravitino.meta;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Field;
import org.apache.gravitino.HasIdentifier;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.stats.StatisticValue;

public abstract class StatisticEntity implements Entity, HasIdentifier, Auditable {
  public static final Field ID =
      Field.required("id", Long.class, "The unique identifier of the statistic entity.");
  public static final Field NAME =
      Field.required("name", String.class, "The name of the statistic entity.");
  public static final Field VALUE =
      Field.required("value", StatisticValue.class, "The value of the statistic entity.");
  public static final Field AUDIT_INFO =
      Field.required("audit_info", Audit.class, "The audit details of the statistic entity.");

  protected Long id;
  protected String name;
  protected StatisticValue<?> value;
  protected AuditInfo auditInfo;
  protected Namespace namespace;

  @Override
  public Audit auditInfo() {
    return auditInfo;
  }

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = Maps.newHashMap();
    fields.put(ID, id);
    fields.put(NAME, name);
    fields.put(VALUE, value);
    fields.put(AUDIT_INFO, auditInfo);
    return fields;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Long id() {
    return id;
  }

  @Override
  public Namespace namespace() {
    return namespace;
  }

  public StatisticValue<?> value() {
    return value;
  }

  public static EntityType getStatisticType(MetadataObject.Type type) {
    switch (type) {
      case TABLE:
        return EntityType.TABLE_STATISTIC;
      default:
        throw new IllegalArgumentException(
            "Unsupported metadata object type for statistics: " + type);
    }
  }

  public static <S extends StatisticEntityBuilder<S, E>, E extends StatisticEntity> S builder(
      Entity.EntityType type) {
    switch (type) {
      case TABLE_STATISTIC:
        return (S) TableStatisticEntity.builder();
      default:
        throw new IllegalArgumentException("Unsupported statistic entity type: " + type);
    }
  }

  interface Builder<SELF extends Builder<SELF, T>, T extends StatisticEntity> {
    T build();
  }

  public abstract static class StatisticEntityBuilder<
          SELF extends Builder<SELF, T>, T extends StatisticEntity>
      implements Builder<SELF, T> {

    protected Long id;
    protected String name;
    protected StatisticValue<?> value;
    protected AuditInfo auditInfo;
    protected Namespace namespace;

    public SELF withId(Long id) {
      this.id = id;
      return self();
    }

    public SELF withName(String name) {
      this.name = name;
      return self();
    }

    public SELF withValue(StatisticValue<?> value) {
      this.value = value;
      return self();
    }

    public SELF withAuditInfo(AuditInfo auditInfo) {
      this.auditInfo = auditInfo;
      return self();
    }

    public SELF withNamespace(Namespace namespace) {
      this.namespace = namespace;
      return self();
    }

    private SELF self() {
      return (SELF) this;
    }

    protected abstract T internalBuild();

    public T build() {
      T t = internalBuild();
      return t;
    }
  }
}
