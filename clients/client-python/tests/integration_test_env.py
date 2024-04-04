"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""

import os
import unittest
import subprocess
import time
import requests

from gravitino import GravitinoClient
from gravitino.constants import TIMEOUT


def check_gravitino_server_status():
    try:
        response = requests.get("http://localhost:8090/api/version")
        response.raise_for_status()  # raise an exception for bad status codes
        response.close()
        return True
    except requests.exceptions.RequestException as e:
        print("Failed to access the server: {}".format(e))
        return False


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

        gravitinoServerRunning = False
        for i in range(10):
            print("Monitoring Gravitino server status. Attempt", i + 1)
            if check_gravitino_server_status():
                print("Gravitino Server is running")
                gravitinoServerRunning = True
                break
            else:
                print("Gravitino Server is not running")
                time.sleep(0.5)

        if not gravitinoServerRunning:
            print("ERROR: Can't start Gravitino server!")
            quit(0)

        cls.client = GravitinoClient("http://localhost:8090", timeout=TIMEOUT)

    @classmethod
    def tearDownClass(cls):
        print("Stop integration test environment...")
        result = subprocess.run([cls.GravitinoHome + '/bin/gravitino.sh', 'stop'], capture_output=True, text=True)
        print('stdout:', result.stdout)
        print('stderr:', result.stderr)

        gravitinoServerRunning = True
        for i in range(10):
            print("Monitoring Gravitino server status. Attempt", i + 1)
            if check_gravitino_server_status():
                print("Gravitino server still running")
                time.sleep(0.5)
            else:
                print("Stop Gravitino server successes!")
                gravitinoServerRunning = False
                break

        if gravitinoServerRunning:
            print("ERROR: Can't stop Gravitino server!")

    # Determine whether to run from Gradle base on environment variables
    # integrated test environment (ITE)
    @staticmethod
    def notInITE():
        return os.environ.get('GRAVITINO_HOME') is None and os.environ.get('PROJECT_VERSION') is None

