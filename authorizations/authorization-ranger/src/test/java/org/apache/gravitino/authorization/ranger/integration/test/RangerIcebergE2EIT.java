/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.authorization.ranger.integration.test;

import com.google.common.collect.Maps;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.integration.test.util.AbstractIT;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

@Tag("gravitino-docker-test")
public class RangerIcebergE2EIT extends AbstractIT {
    private static final Logger LOG = LoggerFactory.getLogger(RangerIcebergE2EIT.class);
    private static final String TEST_USER_NAME = "e2e_it_user";

    @BeforeAll
    public static void startIntegrationTest() throws Exception {
        // Enable Gravitino Authorization mode
        Map<String, String> configs = Maps.newHashMap();
        configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), String.valueOf(true));
        configs.put(Configs.SERVICE_ADMINS.getKey(), RangerITEnv.HADOOP_USER_NAME);
        configs.put(Configs.AUTHENTICATORS.getKey(), AuthenticatorType.SIMPLE.name().toLowerCase());
        configs.put("SimpleAuthUserName", TEST_USER_NAME);
        registerCustomConfigs(configs);
        AbstractIT.startIntegrationTest();

        RangerITEnv.setup();
        RangerITEnv.startHiveRangerContainer();


    }
}
