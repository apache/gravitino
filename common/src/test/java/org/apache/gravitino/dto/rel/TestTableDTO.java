package org.apache.gravitino.dto.rel;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.rel.types.Types;
import org.junit.jupiter.api.Test;

public class TestTableDTO {
  @Test
  public void testBuildWithoutColumns() {
    AuditDTO audit =
        AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    TableDTO.Builder<?> builder = TableDTO.builder().withName("t1").withAudit(audit);
    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  public void testBuildWithEmptyColumns() {
    AuditDTO audit =
        AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    TableDTO.Builder<?> builder =
        TableDTO.builder().withName("t1").withAudit(audit).withColumns(new ColumnDTO[0]);
    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  public void testBuildWithColumns() {
    AuditDTO audit =
        AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    ColumnDTO column =
        ColumnDTO.builder().withName("c1").withDataType(Types.IntegerType.get()).build();
    TableDTO.Builder<?> builder =
        TableDTO.builder().withName("t1").withAudit(audit).withColumns(new ColumnDTO[] {column});
    assertDoesNotThrow(builder::build);
  }
}
