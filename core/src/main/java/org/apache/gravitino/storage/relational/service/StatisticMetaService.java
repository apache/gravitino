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
package org.apache.gravitino.storage.relational.service;

import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.storage.relational.mapper.StatisticMetaMapper;
import org.apache.gravitino.storage.relational.po.StatisticPO;
import org.apache.gravitino.storage.relational.utils.POConverters;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.apache.gravitino.utils.NameIdentifierUtil;

import java.util.List;
import java.util.stream.Collectors;

/** The service class for statistic metadata. It provides the basic database operations for statistic. */
public class StatisticMetaService {

    private static final StatisticMetaService INSTANCE = new StatisticMetaService();

    public static StatisticMetaService getInstance() {
        return INSTANCE;
    }

    private StatisticMetaService() {}


    public List<StatisticPO> listStatisticPOsByObject(NameIdentifier identifier, Entity.EntityType type) {
        long metalakeId =
                MetalakeMetaService.getInstance()
                        .getMetalakeIdByName(NameIdentifierUtil.getMetalake(identifier));
        MetadataObject object = NameIdentifierUtil.toMetadataObject(identifier, type);
        long objectId = MetadataObjectService.getMetadataObjectId(metalakeId, object.fullName(), object.type());
        List<StatisticPO> statisticPOs =
                SessionUtils.getWithoutCommit(
                        StatisticMetaMapper.class,
                        mapper -> mapper.listStatisticPOsByObject(objectId, type.name())
                );
        return statisticPOs.stream()
                .map(statisticPO -> POConverters.fromStatisticPO(statisticPO))
                .collect(Collectors.toList());
    }

    public void batchInsertStatisticPOs(List<StatisticPO> statisticPOs) {

    }

    public void batchUpdateStatisticPOs(List<StatisticPO> statisticPOs) {

    }

    public void batchDeleteStatisticPOs(List<Long> statisticIds) {
        // Implementation for batch deleting StatisticPOs from the database
    }
}
