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

logger = logging.getLogger(__name__)


def get_gravitino_server_version():
    try:
        response = requests.get("http://localhost:8090/api/version")
        response.raise_for_status()  # raise an exception for bad status codes
        response.close()
        return True
    except requests.exceptions.RequestException as e:
        logger.error("Failed to access the server: {}", e)
        return False


def check_gravitino_server_status():
    gravitino_server_running = False
    for i in range(5):
        logger.info("Monitoring Gravitino server status. Attempt %s", i + 1)
        if get_gravitino_server_version():
            logger.debug("Gravitino Server is running")
            gravitino_server_running = True
            break
        else:
            logger.debug("Gravitino Server is not running")
            time.sleep(1)
    return gravitino_server_running


def _init_logging():
    logging.basicConfig(level=logging.DEBUG)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)


# Provide real test environment for the Gravitino Server
class IntegrationTestEnv(unittest.TestCase):
    gravitino_startup_script = None

    @classmethod
    def setUpClass(cls):
        _init_logging()

        if os.environ.get('GRADLE_START_GRAVITINO') is not None:
            logger.info('Manual start gravitino server [%s].', check_gravitino_server_status())
            return

        current_path = os.getcwd()
        cls.gravitino_startup_script = os.path.join(current_path, '../../../distribution/package/bin/gravitino.sh')
        if not os.path.exists(cls.gravitino_startup_script):
            logger.error("Can't find Gravitino startup script: %s, "
                         "Please execute `./gradlew compileDistribution -x test` in the gravitino project root "
                         "directory.", cls.gravitino_startup_script)
            quit(0)

        logger.info("Starting integration test environment...")

        # Start Gravitino Server
        result = subprocess.run([cls.gravitino_startup_script, 'start'], capture_output=True, text=True)
        if result.stdout:
            logger.info("stdout: %s", result.stdout)
        if result.stderr:
            logger.info("stderr: %s", result.stderr)

        if not check_gravitino_server_status():
            logger.error("ERROR: Can't start Gravitino server!")
            quit(0)

    @classmethod
    def tearDownClass(cls):
        if os.environ.get('GRADLE_START_GRAVITINO') is not None:
            return

        logger.info("Stop integration test environment...")
        result = subprocess.run([cls.gravitino_startup_script, 'stop'], capture_output=True, text=True)
        if result.stdout:
            logger.info("stdout: %s", result.stdout)
        if result.stderr:
            logger.info("stderr: %s", result.stderr)

        gravitino_server_running = True
        for i in range(5):
            logger.debug("Monitoring Gravitino server status. Attempt %s", i + 1)
            if check_gravitino_server_status():
                logger.debug("Gravitino server still running")
                time.sleep(1)
            else:
                logger.debug("Stop Gravitino server successes!")
                gravitino_server_running = False
                break

        if gravitino_server_running:
            logger.error("Can't stop Gravitino server!")
