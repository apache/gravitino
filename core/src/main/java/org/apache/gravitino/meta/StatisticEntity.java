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
import org.apache.gravitino.*;

import java.util.Map;

public class StatisticEntity implements Entity, HasIdentifier, Auditable {
    public static final Field ID =
            Field.required("id", Long.class, "The unique identifier of the statistic entity.");
    public static final Field NAME =
            Field.required("name", String.class, "The name of the statistic entity.");
    public static final Field VALUE =
            Field.required("value", String.class, "The value of the statistic entity.");
    public static final Field AUDIT_INFO =
            Field.required("audit_info", Audit.class, "The audit details of the statistic entity.");


    private Long id;
    private String name;
    private String value;
    private AuditInfo auditInfo;
    private Namespace namespace;



    @Override
    public Audit auditInfo() {
        return null;
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
    public EntityType type() {
        return EntityType.STATISTIC;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Long id() {
        return id;
    }

    public static Builder builder() {
        return null;
    }

    public static class Builder {
        private final StatisticEntity statisticEntity;

        private Builder() {
            statisticEntity = new StatisticEntity();
        }

        public Builder withId(Long id) {
            statisticEntity.id = id;
            return this;
        }

        public Builder withName(String name) {
            statisticEntity.name = name;
            return this;
        }

        public Builder withValue(String value) {
            statisticEntity.value = value;
            return this;
        }

        public Builder withAuditInfo(AuditInfo auditInfo) {
            statisticEntity.auditInfo = auditInfo;
            return this;
        }

        public Builder withNamespace(Namespace namespace) {
            statisticEntity.namespace = namespace;
            return this;
        }

        public StatisticEntity build() {
            return statisticEntity;
        }
    }
}
