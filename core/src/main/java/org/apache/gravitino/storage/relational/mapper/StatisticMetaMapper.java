package org.apache.gravitino.storage.relational.mapper;

import java.util.List;
import org.apache.gravitino.storage.relational.po.StatisticPO;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

public interface StatisticMetaMapper {

  String STATISTIC_META_TABLE_NAME = "statistic_meta_version_info";

  @SelectProvider(type = StatisticSQLProviderFactory.class, method = "listStatisticPOsByObject")
  List<StatisticPO> listStatisticPOsByObject(long objectId, String name);

  @InsertProvider(type = StatisticSQLProviderFactory.class, method = "batchInsertStatisticPOs")
  void batchInsertStatisticPOs(List<StatisticPO> statisticPOs);

  @UpdateProvider(type = StatisticSQLProviderFactory.class, method = "batchDeleteStatisticPOs")
  void batchDeleteStatisticPOs(List<Long> statisticIds);
}
