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
  List<StatisticPO> listStatisticPOsByObjectId(@Param("objectId") long objectId);

  @InsertProvider(type = StatisticSQLProviderFactory.class, method = "batchInsertStatisticPOs")
  void batchInsertStatisticPOs(@Param("statisticPOs") List<StatisticPO> statisticPOs);

  @UpdateProvider(type = StatisticSQLProviderFactory.class, method = "batchDeleteStatisticPOs")
  Integer batchDeleteStatisticPOs(@Param("statisticIds") List<Long> statisticIds);

  @UpdateProvider(
      type = StatisticSQLProviderFactory.class,
      method = "softDeleteStatisticsByObjectId")
  Integer softDeleteStatisticsByObjectId(@Param("objectId") Long objectId);

  @UpdateProvider(
      type = StatisticSQLProviderFactory.class,
      method = "softDeleteStatisticsByMetalakeId")
  Integer softDeleteStatisticsByMetalakeId(@Param("metalakeId") Long metalakeId);

  @UpdateProvider(
      type = StatisticSQLProviderFactory.class,
      method = "softDeleteStatisticsByCatalogId")
  Integer softDeleteStatisticsByCatalogId(@Param("catalogId") Long catalogId);

  @UpdateProvider(
      type = StatisticSQLProviderFactory.class,
      method = "softDeleteStatisticsBySchemaId")
  Integer softDeleteStatisticsBySchemaId(@Param("schemaId") Long schemaId);

  @DeleteProvider(
      type = StatisticSQLProviderFactory.class,
      method = "deleteStatisticsByLegacyTimeline")
  Integer deleteStatisticsByLegacyTimeline(
      @Param("legacyTimeline") Long legacyTimeline, @Param("limit") int limit);
}
