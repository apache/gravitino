package org.apache.iceberg;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import org.apache.iceberg.ManifestFile.PartitionFieldSummary;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionsTableExt extends PartitionsTable {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionsTableExt.class);

  public PartitionsTableExt(Table table) {
    super(table);
  }

  /**
   * Get all partitions in the table.
   * @return a collection of partition paths
   */
  public static Collection<String> partitions(Table table, TableScan scan, ExecutorService executorService) {
    List<ManifestFile> manifestFiles = table.currentSnapshot().allManifests(table.io());
    LOG.info("Scanning {} manifest files", manifestFiles.size());
    Set<String> partitionPaths = Collections.synchronizedSet(Sets.newLinkedHashSet());

    Tasks.foreach(manifestFiles)
        .noRetry()
        .suppressFailureWhenFinished()
        .executeWith(executorService)
        .run(manifest -> {
          // todo: cache partition spec id to partition spec
          PartitionSpec partitionSpec = table.specs().get(manifest.partitionSpecId());
          if (hasOnlyOnePartition(manifest, partitionSpec)) {
            String partitionPath = toPartitionPath(manifest, partitionSpec);
            partitionPaths.add(partitionPath);
            LOG.info("Added partition {} from single partition manifest. Collected {} partitions so "
                + "far.", partitionPath, partitionPaths.size());
          } else {
            try (ManifestReader<?> reader =
                ManifestFiles.open(manifest, table.io())
                    .caseSensitive(scan.isCaseSensitive())
                    .select(ImmutableList.of("partition", "lower_bounds", "upper_bounds"))) {
              for (ManifestEntry<?> entry : reader.entries()) {
                ContentFile<?> file = entry.file();
                partitionPaths.add(
                    // todo: get partition spec from cached partition spec
                    table.specs().get(file.specId()).partitionToPath(file.partition()));
              }
              LOG.info("Added multiple partitions from single manifest. Collected {} partitions so "
                  + "far.", partitionPaths.size());
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        });

    LOG.info("Found {} partitions", partitionPaths.size());
    return new ArrayList<>(partitionPaths);
  }

  /**
   * Convert a manifest file to a partition path.
   * @param manifestFile the manifest file
   * @param partitionSpec the partition spec
   * @return the partition path
   */
  private static String toPartitionPath(ManifestFile manifestFile, PartitionSpec partitionSpec) {
    List<Types.NestedField> fields = partitionSpec.partitionType().fields();
    List<PartitionFieldSummary> partitionFieldSummaries = manifestFile.partitions();

    Preconditions.checkState(fields.size() == partitionFieldSummaries.size(),
        "Partition fields and partition field summaries size mismatch");

    PartitionData partitionData = new PartitionData(partitionSpec.partitionType());
    for (int i=0; i < fields.size(); i++) {
      partitionData.put(i, Conversions.fromByteBuffer(fields.get(i).type(),
          partitionFieldSummaries.get(i).lowerBound()));
    }

    return partitionSpec.partitionToPath(partitionData);
  }

  /**
   * Check if the manifest has only one partition.
   * @param manifest the manifest file
   * @param partitionSpec the partition spec
   * @return true if the manifest has only one partition, false otherwise
   */
  private static boolean hasOnlyOnePartition(ManifestFile manifest, PartitionSpec partitionSpec) {
    Preconditions.checkState(partitionSpec.fields().size() == manifest.partitions().size(),
        "Partition fields and partition field summaries size mismatch");
    return Streams.zip(partitionSpec.fields().stream(), manifest.partitions().stream(),
        (field, fieldSummary) -> {
          if (field.transform().isIdentity()) {
            return (fieldSummary.lowerBound() == null && fieldSummary.upperBound() == null) ||
                fieldSummary.lowerBound().equals(fieldSummary.upperBound());
          } else {
            return true;
          }
        }).reduce(true, (a, b) -> a && b);
  }
}
