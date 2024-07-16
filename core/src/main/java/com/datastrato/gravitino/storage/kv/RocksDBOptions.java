/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

import com.datastrato.gravitino.Config;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import lombok.Getter;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.WALRecoveryMode;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksDBOptions {
  public static final Logger LOGGER = LoggerFactory.getLogger(RocksDBKvBackend.class);
  @Getter private final Map<String, BiConsumer<RocksDBOptions, String>> optionSetters;
  @Getter private Options options;

  @Getter private WriteOptions writeOptions;

  @Getter private ReadOptions readOptions;

  public RocksDBOptions() {
    this.options = new Options();
    this.writeOptions = new WriteOptions();
    this.readOptions = new ReadOptions();
    this.optionSetters = new HashMap<>();
    initializeOptionSetters();
  }

  public RocksDBOptions(Options options, WriteOptions writeOptions, ReadOptions readOptions) {
    this.options = options;
    this.writeOptions = writeOptions;
    this.readOptions = readOptions;
    this.optionSetters = new HashMap<>();
    initializeOptionSetters();
  }

  private void initializeOptionSetters() {
    // Each option name maps to a lambda that applies the setting to the appropriate
    // option object
    // and the syntax of optionKey should be:
    // .<className>.<optionName>
    // e.g. .Options.maxBackgroundJobs

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/Options.html#maxBackgroundJobs--
     */
    optionSetters.put(
        ".Options.maxBackgroundJobs",
        (holder, value) -> {
          holder.options.setMaxBackgroundJobs(Integer.parseInt(value));
        });
    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/Options.html#setAllowConcurrentMemtableWrite-boolean-
     */
    optionSetters.put(
        ".Options.allowConcurrentMemtableWrite",
        (holder, value) -> {
          holder.options.setAllowConcurrentMemtableWrite(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/Options.html#setAllowMmapReads-boolean-
     */
    optionSetters.put(
        ".Options.allowMmapReads",
        (holder, value) -> {
          holder.options.setAllowMmapReads(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/Options.html#setAllowMmapWrites-boolean-
     */
    optionSetters.put(
        ".Options.allowMmapWrites",
        (holder, value) -> {
          holder.options.setAllowMmapWrites(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/Options.html#setAvoidFlushDuringRecovery-boolean-
     */
    optionSetters.put(
        ".Options.avoidFlushDuringRecovery",
        (holder, value) -> {
          holder.options.setAvoidFlushDuringRecovery(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/Options.html#setAvoidFlushDuringShutdown-boolean-
     */
    optionSetters.put(
        ".Options.avoidFlushDuringShutdown",
        (holder, value) -> {
          holder.options.setAvoidFlushDuringShutdown(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/Options.html#setAvoidUnnecessaryBlockingIO-boolean-
     */
    optionSetters.put(
        ".Options.avoidUnnecessaryBlockingIO",
        (holder, value) -> {
          holder.options.setAvoidUnnecessaryBlockingIO(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/Options.html#setCreateIfMissing-boolean-
     */
    optionSetters.put(
        ".Options.createIfMissing",
        (holder, value) -> {
          holder.options.setCreateIfMissing(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/Options.html#setCreateMissingColumnFamilies-boolean-
     */
    optionSetters.put(
        ".Options.createMissingColumnFamilies",
        (holder, value) -> {
          holder.options.setCreateMissingColumnFamilies(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/Options.html#setDisableAutoCompactions-boolean-
     */
    optionSetters.put(
        ".Options.disableAutoCompactions",
        (holder, value) -> {
          holder.options.setDisableAutoCompactions(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/Options.html#setEnableThreadTracking-boolean-
     */
    optionSetters.put(
        ".Options.enableThreadTracking",
        (holder, value) -> {
          holder.options.setEnableThreadTracking(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/Options.html#setErrorIfExists-boolean-
     */
    optionSetters.put(
        ".Options.errorIfExists",
        (holder, value) -> {
          holder.options.setErrorIfExists(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/Options.html#setExperimentalMempurgeThreshold-double-
     */
    optionSetters.put(
        ".Options.experimentalMempurgeThreshold",
        (holder, value) -> {
          holder.options.setExperimentalMempurgeThreshold(Double.parseDouble(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/Options.html#setFailIfOptionsFileError-boolean-
     */
    optionSetters.put(
        ".Options.failIfOptionsFileError",
        (holder, value) -> {
          holder.options.setFailIfOptionsFileError(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/Options.html#setParanoidChecks-boolean-
     */
    optionSetters.put(
        ".Options.paranoidChecks",
        (holder, value) -> {
          holder.options.setParanoidChecks(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/Options.html#setReportBgIoStats-boolean-
     */
    optionSetters.put(
        ".Options.reportBgIoStats",
        (holder, value) -> {
          holder.options.setReportBgIoStats(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/Options.html#setSkipCheckingSstFileSizesOnDbOpen-boolean-
     */
    optionSetters.put(
        ".Options.skipCheckingSstFileSizesOnDbOpen",
        (holder, value) -> {
          holder.options.setSkipCheckingSstFileSizesOnDbOpen(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/Options.html#setSkipStatsUpdateOnDbOpen-boolean-
     */
    optionSetters.put(
        ".Options.skipStatsUpdateOnDbOpen",
        (holder, value) -> {
          holder.options.setSkipStatsUpdateOnDbOpen(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/Options.html#setUseDirectIoForFlushAndCompaction-boolean-
     */
    optionSetters.put(
        ".Options.useDirectIoForFlushAndCompaction",
        (holder, value) -> {
          holder.options.setUseDirectIoForFlushAndCompaction(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/Options.html#setUseDirectReads-boolean-
     */
    optionSetters.put(
        ".Options.useDirectReads",
        (holder, value) -> {
          holder.options.setUseDirectReads(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/Options.html#setWalRecoveryMode-org.rocksdb.WALRecoveryMode-
     */
    optionSetters.put(
        ".Options.walRecoveryMode",
        (holder, value) -> {
          holder.options.setWalRecoveryMode(WALRecoveryMode.valueOf(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/ReadOptions.html#setBackgroundPurgeOnIteratorCleanup-boolean-
     */
    optionSetters.put(
        ".ReadOptions.backgroundPurgeOnIteratorCleanup",
        (holder, value) -> {
          holder.readOptions.setBackgroundPurgeOnIteratorCleanup(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/ReadOptions.html#setFillCache-boolean-
     */
    optionSetters.put(
        ".ReadOptions.fillCache",
        (holder, value) -> {
          holder.readOptions.setFillCache(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/ReadOptions.html#setIgnoreRangeDeletions-boolean-
     */
    optionSetters.put(
        ".ReadOptions.ignoreRangeDeletions",
        (holder, value) -> {
          holder.readOptions.setIgnoreRangeDeletions(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/ReadOptions.html#setReadaheadSize-long-
     */
    optionSetters.put(
        ".ReadOptions.readaheadSize",
        (holder, value) -> {
          holder.readOptions.setReadaheadSize(Long.parseLong(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/ReadOptions.html#setTailing-boolean-
     */
    optionSetters.put(
        ".ReadOptions.tailing",
        (holder, value) -> {
          holder.readOptions.setTailing(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/ReadOptions.html#setTotalOrderSeek-boolean-
     */
    optionSetters.put(
        ".ReadOptions.totalOrderSeek",
        (holder, value) -> {
          holder.readOptions.setTotalOrderSeek(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/ReadOptions.html#setVerifyChecksums-boolean-
     */
    optionSetters.put(
        ".ReadOptions.verifyChecksums",
        (holder, value) -> {
          holder.readOptions.setVerifyChecksums(Boolean.parseBoolean(value));
        });

    /** WriteOptions */

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/WriteOptions.html#setDisableWAL-boolean-
     */
    optionSetters.put(
        ".WriteOptions.disableWAL",
        (holder, value) -> {
          holder.writeOptions.setDisableWAL(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/WriteOptions.html#setIgnoreMissingColumnFamilies-boolean-
     */
    optionSetters.put(
        ".WriteOptions.ignoreMissingColumnFamilies",
        (holder, value) -> {
          holder.writeOptions.setIgnoreMissingColumnFamilies(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/WriteOptions.html#setLowPri-boolean-
     */
    optionSetters.put(
        ".WriteOptions.lowPri",
        (holder, value) -> {
          holder.writeOptions.setLowPri(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/WriteOptions.html#setMemtableInsertHintPerBatch-boolean-
     */
    optionSetters.put(
        ".WriteOptions.memtableInsertHintPerBatch",
        (holder, value) -> {
          holder.writeOptions.setMemtableInsertHintPerBatch(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/WriteOptions.html#setNoSlowdown-boolean-
     */
    optionSetters.put(
        ".WriteOptions.noSlowdown",
        (holder, value) -> {
          holder.writeOptions.setNoSlowdown(Boolean.parseBoolean(value));
        });

    /**
     * https://javadoc.io/doc/org.rocksdb/rocksdbjni/7.10.2/org/rocksdb/WriteOptions.html#setSync-boolean-
     */
    optionSetters.put(
        ".WriteOptions.sync",
        (holder, value) -> {
          holder.writeOptions.setSync(Boolean.parseBoolean(value));
        });
  }

  //
  /**
   * Apply user-defined options to option if this options is configurable. TODO: List all
   * configurable options.
   */
  public void setOptions(Config config) {
    String prefix = "gravitino.entity.store.kv.rocksdb";
    Map<String, String> configMap = config.getConfigsWithPrefix(prefix);
    optionSetters.forEach(
        (optionKey, optionValue) -> {
          String originalOptionKey = prefix + optionKey;
          if (configMap.containsKey(originalOptionKey)) {
            optionValue.accept(this, configMap.get(originalOptionKey));
          }
        });
  }
}
