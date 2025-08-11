package org.apache.gravitino.dto.rel.indexes;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestIndexDTO {

  @Test
  public void testHashCodeConsistentWithEquals() {
    String[][] fields1 = new String[][] {{"a"}, {"b"}};
    String[][] fields2 = new String[][] {{"a"}, {"b"}};

    IndexDTO index1 =
        IndexDTO.builder()
            .withIndexType(IndexDTO.IndexType.PRIMARY_KEY)
            .withName("idx")
            .withFieldNames(fields1)
            .build();

    IndexDTO index2 =
        IndexDTO.builder()
            .withIndexType(IndexDTO.IndexType.PRIMARY_KEY)
            .withName("idx")
            .withFieldNames(fields2)
            .build();

    Assertions.assertEquals(index1, index2);
    Assertions.assertEquals(index1.hashCode(), index2.hashCode());
  }
}
