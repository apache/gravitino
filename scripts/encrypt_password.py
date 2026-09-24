#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""
Gravitino Password Encryption/Decryption Tool.

Compatible with the Java PasswordEncryptor (AES-256-GCM + PBKDF2WithHmacSHA256).
Encrypted values use the format: ENC(base64(salt[16] + iv[12] + ciphertext + tag))

Usage:
    # Encrypt (master key from env var)
    export GRAVITINO_PASSWORD_ENCRYPTION_KEY="your-master-key"
    python3 encrypt_password.py encrypt "myPassword"

    # Encrypt (master key as option)
    python3 encrypt_password.py encrypt "myPassword" --key "your-master-key"

    # Interactive mode (password will be prompted)
    python3 encrypt_password.py encrypt --key "your-master-key"

    # Decrypt
    python3 encrypt_password.py decrypt "ENC(base64data)" --key "your-master-key"

Requirements:
    pip install cryptography
"""

import argparse
import base64
import getpass
import hashlib
import os
import sys

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
except ImportError:
    sys.stderr.write(
        "Error: 'cryptography' package is required. Install it with:\n"
        "  pip install cryptography\n"
    )
    sys.exit(1)


ENV_KEY = "GRAVITINO_PASSWORD_ENCRYPTION_KEY"
DEFAULT_ENCRYPTION_KEY = "gravitino"
SALT_LENGTH = 16
IV_LENGTH = 12
KEY_LENGTH = 32  # 256 bits
PBKDF2_ITERATIONS = 65536
ENC_PREFIX = "ENC("
ENC_SUFFIX = ")"


def derive_key(master_password: str, salt: bytes) -> bytes:
    """Derive an AES-256 key from master password and salt using PBKDF2-HMAC-SHA256."""
    return hashlib.pbkdf2_hmac(
        "sha256",
        master_password.encode("utf-8"),
        salt,
        PBKDF2_ITERATIONS,
        dklen=KEY_LENGTH,
    )


def is_encrypted(value: str) -> bool:
    """Check whether the value is in ENC(...) format."""
    if not value or not value.strip():
        return False
    trimmed = value.strip()
    return trimmed.startswith(ENC_PREFIX) and trimmed.endswith(ENC_SUFFIX)


def encrypt(plaintext: str, master_key: str) -> str:
    """Encrypt a plaintext password to ENC(base64(...)) format."""
    salt = os.urandom(SALT_LENGTH)
    iv = os.urandom(IV_LENGTH)
    key = derive_key(master_key, salt)

    aesgcm = AESGCM(key)
    ciphertext_and_tag = aesgcm.encrypt(iv, plaintext.encode("utf-8"), None)

    combined = salt + iv + ciphertext_and_tag
    encoded = base64.b64encode(combined).decode("ascii")
    return f"{ENC_PREFIX}{encoded}{ENC_SUFFIX}"


def decrypt(encrypted_value: str, master_key: str) -> str:
    """Decrypt an ENC(...) formatted value."""
    if not is_encrypted(encrypted_value):
        raise ValueError("Value is not in ENC(...) format")

    trimmed = encrypted_value.strip()
    base64_content = trimmed[len(ENC_PREFIX):-len(ENC_SUFFIX)]
    decoded = base64.b64decode(base64_content)

    if len(decoded) < SALT_LENGTH + IV_LENGTH + 1:
        raise ValueError("Encrypted data is too short")

    salt = decoded[:SALT_LENGTH]
    iv = decoded[SALT_LENGTH:SALT_LENGTH + IV_LENGTH]
    ciphertext_and_tag = decoded[SALT_LENGTH + IV_LENGTH:]

    key = derive_key(master_key, salt)
    aesgcm = AESGCM(key)
    plaintext = aesgcm.decrypt(iv, ciphertext_and_tag, None)
    return plaintext.decode("utf-8")


def resolve_master_key(cli_key=None):
    """Resolve master key from CLI arg, env var, or default key."""
    if cli_key:
        return cli_key
    key = os.environ.get(ENV_KEY)
    if key:
        return key
    return DEFAULT_ENCRYPTION_KEY


def cmd_encrypt(args):
    master_key = resolve_master_key(args.key)
    if master_key == DEFAULT_ENCRYPTION_KEY:
        sys.stderr.write(
            f"Warning: Using default master key \"{DEFAULT_ENCRYPTION_KEY}\". "
            f"Set a custom key via --key option or environment variable {ENV_KEY} "
            f"for production use.\n"
        )

    plaintext = args.password
    if not plaintext:
        plaintext = getpass.getpass("Enter password to encrypt: ")

    if not plaintext:
        sys.stderr.write("Error: Password to encrypt must not be empty.\n")
        sys.exit(1)

    encrypted = encrypt(plaintext, master_key)
    print(encrypted)


def cmd_decrypt(args):
    master_key = resolve_master_key(args.key)

    encrypted_value = args.value
    if not encrypted_value:
        encrypted_value = input("Enter encrypted value (ENC(...)): ")

    if not is_encrypted(encrypted_value):
        sys.stderr.write("Error: Value is not in ENC(...) format.\n")
        sys.exit(1)

    try:
        decrypted = decrypt(encrypted_value, master_key)
        print(decrypted)
    except Exception as e:
        sys.stderr.write(f"Error: Failed to decrypt - {e}\n")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Gravitino Password Encryption/Decryption Tool (AES-256-GCM)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Encrypt with master key from environment
  export %s="my-secret-key"
  python3 encrypt_password.py encrypt "myPassword"

  # Encrypt with explicit master key
  python3 encrypt_password.py encrypt "myPassword" --key "my-secret-key"

  # Interactive mode
  python3 encrypt_password.py encrypt --key "my-secret-key"

  # Decrypt
  python3 encrypt_password.py decrypt "ENC(base64data)" --key "my-secret-key"
""" % ENV_KEY,
    )

    subparsers = parser.add_subparsers(dest="action", metavar="ACTION")

    encrypt_parser = subparsers.add_parser("encrypt", help="Encrypt a password")
    encrypt_parser.add_argument("password", nargs="?", help="Password to encrypt")
    encrypt_parser.add_argument(
        "--key",
        help="Master encryption key (default: env var %s or '%s')"
        % (ENV_KEY, DEFAULT_ENCRYPTION_KEY),
    )
    encrypt_parser.set_defaults(func=cmd_encrypt)

    decrypt_parser = subparsers.add_parser("decrypt", help="Decrypt an ENC(...) value")
    decrypt_parser.add_argument("value", nargs="?", help="Encrypted value ENC(...)")
    decrypt_parser.add_argument(
        "--key",
        help="Master encryption key (default: env var %s or '%s')"
        % (ENV_KEY, DEFAULT_ENCRYPTION_KEY),
    )
    decrypt_parser.set_defaults(func=cmd_decrypt)

    args = parser.parse_args()
    if not args.action:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
