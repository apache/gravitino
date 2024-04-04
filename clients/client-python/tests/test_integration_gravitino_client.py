"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import os
import unittest
from tests.integration_test_env import IntegrationTestEnv


@unittest.skipIf(IntegrationTestEnv.notInITE(),
                 "Currently, Python Client integration test only runs in the Gradle build environment")
class IntegrationTestGravitinoClient(IntegrationTestEnv):
    def test_version(self):
        versionDTO = self.client.version
        assert versionDTO['version'] is not None

        # Get project version from environment (Setting by Gradle build script `build.gradle.kts`),
        # But if you directly execute this test in IDEA, maybe can not get it.
        projectVersion = os.environ.get('PROJECT_VERSION', '')

        if projectVersion != '':
            assert versionDTO['version'] == projectVersion
