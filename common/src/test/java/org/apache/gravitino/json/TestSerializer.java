package org.apache.gravitino.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.gravitino.dto.rel.expressions.LiteralDTO;
import org.apache.gravitino.dto.rel.partitioning.DayPartitioningDTO;
import org.apache.gravitino.dto.rel.partitioning.RangePartitioningDTO;
import org.apache.gravitino.dto.rel.partitions.RangePartitionDTO;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Distributions.DistributionImpl;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.sorts.NullOrdering;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.sorts.SortOrders.SortImpl;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index.IndexType;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.indexes.Indexes.IndexImpl;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class TestSerializer {

  @Test
  void testDistributionImplSerializer() throws JsonProcessingException {
    DistributionImpl distribution =
        (DistributionImpl) Distributions.of(Strategy.EVEN, 10, NamedReference.field("col1"));
    String actualJson = JsonUtils.anyFieldMapper().writeValueAsString(distribution);
    String expectedJson =
        """
            {"strategy":"even","number":10,"expressions":[{"type":"field","fieldName":["col1"]}]}""";
    Assertions.assertEquals(expectedJson, actualJson);
    DistributionImpl deserialized =
        JsonUtils.anyFieldMapper().readValue(actualJson, DistributionImpl.class);
    Assertions.assertEquals(distribution, deserialized);

    distribution =
        (DistributionImpl)
            Distributions.of(
                Strategy.EVEN,
                10,
                FunctionExpression.of(
                    "bucket", Literals.integerLiteral(10), NamedReference.field("col_1")));
    actualJson = JsonUtils.anyFieldMapper().writeValueAsString(distribution);
    expectedJson =
        """
          {"strategy":"even","number":10,"expressions":[{"type":"function","funcName":"bucket","funcArgs":[{"type":"literal","dataType":"integer","value":"10"},{"type":"field","fieldName":["col_1"]}]}]}""";
    Assertions.assertEquals(expectedJson, actualJson);
    deserialized = JsonUtils.anyFieldMapper().readValue(actualJson, DistributionImpl.class);
    Assertions.assertEquals(distribution, deserialized);
  }

  @Test
  void testSortOrderSerializer() throws JsonProcessingException {
    SortImpl sortOrder =
        SortOrders.of(
            NamedReference.field("col1"), SortDirection.ASCENDING, NullOrdering.NULLS_LAST);
    String actualJson = JsonUtils.anyFieldMapper().writeValueAsString(sortOrder);
    String expectedJson =
        """
            {"sortTerm":{"type":"field","fieldName":["col1"]},"direction":"asc","nullOrdering":"nulls_last"}""";
    Assertions.assertEquals(expectedJson, actualJson);

    SortImpl deserialized = JsonUtils.anyFieldMapper().readValue(actualJson, SortImpl.class);
    Assertions.assertEquals(sortOrder, deserialized);

    sortOrder =
        SortOrders.of(
            FunctionExpression.of("lower", NamedReference.field("col_1")),
            SortDirection.DESCENDING);
    actualJson = JsonUtils.anyFieldMapper().writeValueAsString(sortOrder);
    expectedJson =
        """
          {"sortTerm":{"type":"function","funcName":"lower","funcArgs":[{"type":"field","fieldName":["col_1"]}]},"direction":"desc","nullOrdering":"nulls_last"}""";
    Assertions.assertEquals(expectedJson, actualJson);
    deserialized = JsonUtils.anyFieldMapper().readValue(actualJson, SortImpl.class);
    Assertions.assertEquals(sortOrder, deserialized);
  }

  @Test
  void testIndexImplSerializer() throws JsonProcessingException {
    IndexImpl index =
        (IndexImpl)
            Indexes.of(IndexType.PRIMARY_KEY, "index_1", new String[][] {new String[] {"col1"}});

    String actualJson = JsonUtils.anyFieldMapper().writeValueAsString(index);
    String expectedJson =
        """
            {"indexType":"PRIMARY_KEY","name":"index_1","fieldNames":[["col1"]]}""";
    Assertions.assertEquals(expectedJson, actualJson);
    IndexImpl deserialized = JsonUtils.anyFieldMapper().readValue(actualJson, IndexImpl.class);
    Assertions.assertEquals(index, deserialized);

    index =
        (IndexImpl)
            Indexes.of(
                IndexType.UNIQUE_KEY,
                "index_2",
                new String[][] {new String[] {"col1"}, new String[] {"col2"}});
    actualJson = JsonUtils.anyFieldMapper().writeValueAsString(index);
    expectedJson =
        """
            {"indexType":"UNIQUE_KEY","name":"index_2","fieldNames":[["col1"],["col2"]]}""";
    Assertions.assertEquals(expectedJson, actualJson);
  }

  @Test
  @Disabled("Disable until Partitioning serializer is implemented")
  void testPartitioningSerializer() throws JsonProcessingException {
    Transform transform = DayPartitioningDTO.of("dt");
    String actualJson = JsonUtils.anyFieldMapper().writeValueAsString(transform);
    String expectedJson = """
          {"strategy":"day","fieldName":["dt"]}""";
    Assertions.assertEquals(expectedJson, actualJson);

    Transform deserialized = JsonUtils.anyFieldMapper().readValue(actualJson, Transform.class);
    Assertions.assertEquals(transform, deserialized);

    transform =
        RangePartitioningDTO.of(
            new String[] {"dt"},
            new RangePartitionDTO[] {
              RangePartitionDTO.builder()
                  .withName("p1")
                  .withLower(
                      LiteralDTO.builder()
                          .withValue("2023-01-01")
                          .withDataType(Types.StringType.get())
                          .build())
                  .withUpper(
                      LiteralDTO.builder()
                          .withValue("2024-01-01")
                          .withDataType(Types.StringType.get())
                          .build())
                  .build(),
            });

    actualJson = JsonUtils.anyFieldMapper().writeValueAsString(transform);
    expectedJson =
        """
            {"type":"range_partitioning","fieldName":["dt"],"assignments":[{"name":"p1","properties":null,"upper":{"type":"literal","dataType":"string","value":"2024-01-01"},"lower":{"type":"literal","dataType":"string","value":"2023-01-01"}}]}""";
    Assertions.assertEquals(expectedJson, actualJson);
    deserialized = JsonUtils.anyFieldMapper().readValue(actualJson, Transform.class);
    Assertions.assertEquals(transform, deserialized);
  }
}
