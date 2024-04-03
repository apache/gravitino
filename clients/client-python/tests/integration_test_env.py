"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
import os
import unittest
import subprocess
import time

from gravitino import GravitinoClient
from gravitino.constants import TIMEOUT

# Provide real test environment for the Gravitino Server
class IntegrationTestEnv(unittest.TestCase):
    GravitinoHome = None

    @classmethod
    def setUpClass(cls):
        print("Starting integration test environment...")
        cls.GravitinoHome = os.environ.get('GRAVITINO_HOME')
        if cls.GravitinoHome is None:
            print('WARN: Currently, Python Client integration test only runs in the Gradle build environment, '
                  'Please execute `./gradlew :clients:client-python:test` in the gravitino project root directory.')
            quit(0)

        # Start Gravitino Server
        result = subprocess.run([cls.GravitinoHome + '/bin/gravitino.sh', 'start'], capture_output=True, text=True)
        print('stdout:', result.stdout)
        print('stderr:', result.stderr)
        time.sleep(3)
        cls.client = GravitinoClient("http://localhost:8090", timeout=TIMEOUT)

    @classmethod
    def tearDownClass(cls):
        print("Stop integration test environment...")
        result = subprocess.run([cls.GravitinoHome + '/bin/gravitino.sh', 'stop'], capture_output=True, text=True)
        print('stdout:', result.stdout)
        print('stderr:', result.stderr)
        time.sleep(3)

    # Determine whether to run from Gradle base on environment variables
    # integrated test environment (ITE)
    @staticmethod
    def notInITE():
        return os.environ.get('GRAVITINO_HOME') is None and os.environ.get('PROJECT_VERSION') is None

