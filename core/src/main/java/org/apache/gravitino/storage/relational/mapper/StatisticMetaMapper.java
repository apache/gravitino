package org.apache.gravitino.storage.relational.mapper;

import java.util.List;
import org.apache.gravitino.storage.relational.po.StatisticPO;
import org.apache.ibatis.annotations.DeleteProvider;
import org.apache.ibatis.annotations.InsertProvider;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.SelectProvider;
import org.apache.ibatis.annotations.UpdateProvider;

public interface StatisticMetaMapper {

  String STATISTIC_META_TABLE_NAME = "statistic_meta";

  @SelectProvider(type = StatisticSQLProviderFactory.class, method = "listStatisticPOsByObjectId")
  List<StatisticPO> listStatisticPOsByObjectId(
      @Param("objectId") long objectId, @Param("name") String name);

  @InsertProvider(type = StatisticSQLProviderFactory.class, method = "batchInsertStatisticPOs")
  void batchInsertStatisticPOs(@Param("statisticPOs") List<StatisticPO> statisticPOs);

  @UpdateProvider(type = StatisticSQLProviderFactory.class, method = "batchDeleteStatisticPOs")
  String batchDeleteStatisticPOs(@Param("statisticIds") List<Long> statisticIds);

  @DeleteProvider(
      type = StatisticSQLProviderFactory.class,
      method = "deleteStatisticsByLegacyTimeline")
  Integer deleteStatisticsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
