package org.apache.gravitino.stats;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPartitionRange {

  @Test
  public void testPartitionRange() {
    PartitionRange range1 = PartitionRange.lessThan("upper");
    Assertions.assertTrue(range1.upperPartitionName.isPresent());
    Assertions.assertFalse(range1.lowerPartitionName.isPresent());
    Assertions.assertEquals("upper", range1.upperPartitionName.get());
    Assertions.assertEquals(PartitionComparator.Type.NAME, range1.comparator().type());

    PartitionRange range2 = PartitionRange.greaterOrEqual("lower");
    Assertions.assertFalse(range2.upperPartitionName.isPresent());
    Assertions.assertTrue(range2.lowerPartitionName.isPresent());
    Assertions.assertEquals("lower", range2.lowerPartitionName.get());
    Assertions.assertEquals(PartitionComparator.Type.NAME, range2.comparator().type());

    PartitionRange range3 = PartitionRange.greaterOrEqual("lower", PartitionComparator.Type.NAME);
    Assertions.assertTrue(range3.lowerPartitionName.isPresent());
    Assertions.assertFalse(range3.upperPartitionName.isPresent());
    Assertions.assertEquals("lower", range3.lowerPartitionName.get());
    Assertions.assertEquals(PartitionComparator.Type.NAME, range3.comparator().type());

    PartitionRange range4 = PartitionRange.lessThan("upper", PartitionComparator.Type.NAME);
    Assertions.assertTrue(range4.upperPartitionName.isPresent());
    Assertions.assertFalse(range4.lowerPartitionName.isPresent());
    Assertions.assertEquals("upper", range4.upperPartitionName.get());
    Assertions.assertEquals(PartitionComparator.Type.NAME, range4.comparator().type());

    PartitionRange range5 = PartitionRange.between("lower", "upper", PartitionComparator.Type.NAME);
    Assertions.assertTrue(range5.lowerPartitionName.isPresent());
    Assertions.assertTrue(range5.upperPartitionName.isPresent());
    Assertions.assertEquals("lower", range5.lowerPartitionName.get());
    Assertions.assertEquals("upper", range5.upperPartitionName.get());
    Assertions.assertEquals(PartitionComparator.Type.NAME, range5.comparator().type());

    PartitionRange range6 = PartitionRange.between("lower", "upper");
    Assertions.assertTrue(range6.lowerPartitionName.isPresent());
    Assertions.assertTrue(range6.upperPartitionName.isPresent());
    Assertions.assertEquals("lower", range6.lowerPartitionName.get());
    Assertions.assertEquals("upper", range6.upperPartitionName.get());
    Assertions.assertEquals(PartitionComparator.Type.NAME, range6.comparator().type());
  }
}
