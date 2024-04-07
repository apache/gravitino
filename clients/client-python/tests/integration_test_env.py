"""
Copyright 2024 Datastrato Pvt Ltd.
This software is licensed under the Apache License version 2.
"""
import logging
import os
import unittest
import subprocess
import time
import requests

from gravitino import GravitinoClient
from gravitino.constants import TIMEOUT

logger = logging.getLogger(__name__)


def check_gravitino_server_status():
    try:
        response = requests.get("http://localhost:8090/api/version")
        response.raise_for_status()  # raise an exception for bad status codes
        response.close()
        return True
    except requests.exceptions.RequestException as e:
        logger.error("Failed to access the server: {}", e)
        return False


def _init_logging():
    logging.basicConfig(level=logging.DEBUG)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)


# Provide real test environment for the Gravitino Server
class IntegrationTestEnv(unittest.TestCase):
    GravitinoHome = None

    @classmethod
    def setUpClass(cls):
        _init_logging()
        logger.info("Starting integration test environment...")
        cls.GravitinoHome = os.environ.get('GRAVITINO_HOME')
        if cls.GravitinoHome is None:
            logger.warning('WARN: Currently, Python Client integration test only runs in the Gradle build environment, '
                  'Please execute `./gradlew :clients:client-python:test` in the gravitino project root directory.')
            quit(0)

        # Start Gravitino Server
        result = subprocess.run([cls.GravitinoHome + '/bin/gravitino.sh', 'start'], capture_output=True, text=True)
        if result.stdout:
            logger.info('stdout:', result.stdout)
        if result.stderr:
            logger.info('stderr:', result.stderr)

        gravitinoServerRunning = False
        for i in range(5):
            logger.info("Monitoring Gravitino server status. Attempt {}", i + 1)
            if check_gravitino_server_status():
                logger.debug("Gravitino Server is running")
                gravitinoServerRunning = True
                break
            else:
                logger.debug("Gravitino Server is not running")
                time.sleep(1)

        if not gravitinoServerRunning:
            logger.error("ERROR: Can't start Gravitino server!")
            quit(0)

        cls.client = GravitinoClient("http://localhost:8090", timeout=TIMEOUT)

    @classmethod
    def tearDownClass(cls):
        logger.info("Stop integration test environment...")
        result = subprocess.run([cls.GravitinoHome + '/bin/gravitino.sh', 'stop'], capture_output=True, text=True)
        if result.stdout:
            logger.debug('stdout:', result.stdout)
        if result.stderr:
            logger.debug('stderr:', result.stderr)

        gravitinoServerRunning = True
        for i in range(5):
            logger.debug("Monitoring Gravitino server status. Attempt {}", i + 1)
            if check_gravitino_server_status():
                logger.debug("Gravitino server still running")
                time.sleep(1)
            else:
                logger.debug("Stop Gravitino server successes!")
                gravitinoServerRunning = False
                break

        if gravitinoServerRunning:
            logger.error("ERROR: Can't stop Gravitino server!")

    # Determine whether to run from Gradle base on environment variables
    # integrated test environment (ITE)
    @staticmethod
    def not_in_ITE():
        return os.environ.get('GRAVITINO_HOME') is None and os.environ.get('PROJECT_VERSION') is None
