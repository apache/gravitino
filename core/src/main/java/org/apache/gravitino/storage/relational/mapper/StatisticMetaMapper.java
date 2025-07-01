package org.apache.gravitino.storage.relational.mapper;

import org.apache.gravitino.meta.StatisticEntity;
import org.apache.gravitino.storage.relational.po.StatisticPO;

import java.util.List;

public class StatisticMetaMapper {

    String STATISTIC_META_TABLE_NAME = "statistic_meta_version_info";


    public List<StatisticPO> listStatisticPOsByObject(long objectId, String name) {
        return null;
    }

    public Object batchInsertStatisticPOs(List<StatisticEntity> statisticEntities) {
        return null;
    }

    public void batchDeleteStatisticPOs(List<Long> statisticIds) {

    }
}
