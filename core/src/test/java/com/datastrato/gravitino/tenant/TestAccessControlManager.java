/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.tenant;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.TestEntityStore;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class TestAccessControlManager {

    private static AccessControlManager accessControlManager;

    private static EntityStore entityStore;

    private static Config config;


    @BeforeAll
    public static void setUp() {
        config = new Config(false) {};

        entityStore = new TestEntityStore.InMemoryEntityStore();
        entityStore.initialize(config);
        entityStore.setSerDe(null);

        accessControlManager = new AccessControlManager(entityStore, new RandomIdGenerator());
    }

    @AfterAll
    public static void tearDown() throws IOException {
        if (entityStore != null) {
            entityStore.close();
            entityStore = null;
        }
    }

    @Test
    public void testCreateUser() {

    }

    @Test
    public void testLoadUser() {

    }

    @Test
    public void testAlterUser() {

    }
}
