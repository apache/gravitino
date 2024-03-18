package com.datastrato.gravitino.storage.kv;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.config.ConfigBuilder;
import com.datastrato.gravitino.config.ConfigEntry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;


public class TestRocksDBOptions {
    @Test
    void testRocksDBOptions() {
        Config config = Mockito.mock(Config.class);

        String prefix = "gravitino.entity.store.kv.rocksdb.options";
        String optionsKey = prefix + "." + "createIfMissing";
        ConfigEntry<Boolean> createIfMissingConf =
                new ConfigBuilder(optionsKey).booleanConf();
        Map<String, String> mockConfigMap = new HashMap<String, String>();
        mockConfigMap.put(optionsKey, "true");

        Mockito.when(config.getConfigsWithPrefix(prefix)).thenReturn(mockConfigMap);
        RocksDBOptions options = new RocksDBOptions();
        options.setOptions(config);
        // We haven't set the GRAVITINO_HOME
        Assertions.assertDoesNotThrow(() -> options.setOptions(config));
    }
}
