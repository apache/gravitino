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
package org.apache.gravitino.storage.relational.po;

import com.google.common.base.Preconditions;

import java.util.Objects;

public class StatisticPO {
    private Long metalakeId;
    private Long statisticId;

    private String statisticName;

    private String value;
    private Long objectId;

    private String objectType;

    private String auditInfo;

    private Long currentVersion;
    private Long lastVersion;
    private Long deletedAt;
    private StatisticPO() {}

    public static Builder builder() {
        return new Builder();
    }

    public Long getMetalakeId() { return metalakeId; }
    public Long getStatisticId() {
        return statisticId;
    }

    public Long getObjectId() {
        return objectId;
    }

    public String getObjectType() {
        return objectType;
    }

    public String getStatisticName() {
        return statisticName;
    }

    public String getValue() {
        return value;
    }

    public String getAuditInfo() {
        return auditInfo;
    }

    public Long getCurrentVersion() {
        return currentVersion;
    }

    public Long getLastVersion() {
        return lastVersion;
    }

    public Long getDeletedAt() {
        return deletedAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StatisticPO)) {
            return false;
        }
        StatisticPO that = (StatisticPO) o;
        return statisticId.equals(that.statisticId)
                && objectId.equals(that.objectId)
                && metalakeId.equals(that.metalakeId)
                && objectType.equals(that.objectType)
                && statisticName.equals(that.statisticName)
                && value.equals(that.value)
                && auditInfo.equals(that.auditInfo)
                && currentVersion.equals(that.currentVersion)
                && lastVersion.equals(that.lastVersion)
                && deletedAt.equals(that.deletedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metalakeId,
                statisticId,
                objectId,
                objectType,
                statisticName,
                value,
                auditInfo,
                currentVersion,
                lastVersion,
                deletedAt);
    }

    public static class Builder {

        private final StatisticPO statisticPO;

        public Builder() {
            this.statisticPO = new StatisticPO();
        }

        public Builder withMetalakeId(Long metalakeId) {
            statisticPO.metalakeId = metalakeId;
            return this;
        }

        public Builder withStatisticId(Long statisticId) {
            statisticPO.statisticId = statisticId;
            return this;
        }

        public Builder withObjectId(Long objectId) {
            statisticPO.objectId = objectId;
            return this;
        }

        public Builder withObjectType(String objectType) {
            statisticPO.objectType = objectType;
            return this;
        }

        public Builder withStatisticName(String statisticName) {
            statisticPO.statisticName = statisticName;
            return this;
        }

        public Builder withValue(String value) {
            statisticPO.value = value;
            return this;
        }

        public Builder withAuditInfo(String auditInfo) {
            statisticPO.auditInfo = auditInfo;
            return this;
        }

        public Builder withCurrentVersion(Long currentVersion) {
            statisticPO.currentVersion = currentVersion;
            return this;
        }

        public Builder withLastVersion(Long lastVersion) {
            statisticPO.lastVersion = lastVersion;
            return this;
        }

        public Builder withDeletedAt(Long deletedAt) {
            statisticPO.deletedAt = deletedAt;
            return this;
        }

        public StatisticPO build() {
            Preconditions.checkArgument(statisticPO.objectId != null, "`objectId is required");
            Preconditions.checkArgument(
                    statisticPO.objectType != null, "`objectType` is required");
            Preconditions.checkArgument(
                    statisticPO.statisticName != null, "`statisticName` is required");
            Preconditions.checkArgument(statisticPO.value != null, "`value` is required");
            Preconditions.checkArgument(
                    statisticPO.auditInfo != null, "`auditInfo` is required");
            return statisticPO;
        }
    }
}
