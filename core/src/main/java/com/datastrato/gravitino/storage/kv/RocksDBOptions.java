/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.kv;

import com.datastrato.gravitino.Config;
import org.rocksdb.WriteOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.Options;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksDBOptions {
    public static final Logger LOGGER = LoggerFactory.getLogger(RocksDBKvBackend.class);
    private final Map<String, BiConsumer<RocksDBOptions, String>> optionSetters;
    @Getter
    private Options options;

    @Getter
    private WriteOptions writeOptions;

    @Getter
    private ReadOptions readOptions;


    public RocksDBOptions() {
        this.options = new Options();
        this.writeOptions = new WriteOptions();
        this.readOptions = new ReadOptions();
        this.optionSetters = new HashMap<>();
        initializeOptionSetters();
    }

    private void initializeOptionSetters() {
        // Each option name maps to a lambda that applies the setting to the appropriate option object
        optionSetters.put("gravitino.entity.store.kv.rocksdb.options.createIfMissing", (holder, value) -> holder.options.setCreateIfMissing(Boolean.parseBoolean(value)));
        // Add more options here. For example:
        // optionSetters.put("maxOpenFiles", (holder, value) -> holder.options.setMaxOpenFiles(Integer.parseInt(value)));
    }

    public void setOptions(Config config) {
        String prefix = "gravitino.entity.store.kv.rocksdb.options";
        Map<String, String> configMap = config.getConfigsWithPrefix(prefix);
        configMap.forEach((optionKey, optionValue) -> {
            if (this.optionSetters.containsKey(optionKey)) {
                this.optionSetters.get(optionKey).accept(this, optionValue);
            } else {
                throw new IllegalArgumentException("Option " + optionKey + " is not supported.");
            }
        });
        LOGGER.debug("ZZZ Options: {}", this.options);
    }
}
