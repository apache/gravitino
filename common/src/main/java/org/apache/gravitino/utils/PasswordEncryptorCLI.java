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
package org.apache.gravitino.utils;

import java.nio.charset.StandardCharsets;
import java.util.Scanner;

/**
 * Command-line tool for encrypting and decrypting passwords for use in Gravitino configuration
 * files.
 *
 * <p>Usage:
 *
 * <pre>
 * # Encrypt a password (master key from env var)
 * export GRAVITINO_PASSWORD_ENCRYPTION_KEY="your-master-key"
 * java -cp libs/gravitino-common-*.jar org.apache.gravitino.utils.PasswordEncryptorCLI encrypt
 *
 * # Encrypt a password (master key as argument)
 * java -cp libs/gravitino-common-*.jar org.apache.gravitino.utils.PasswordEncryptorCLI \
 *   encrypt --key "your-master-key"
 *
 * # Encrypt a password directly
 * java -cp libs/gravitino-common-*.jar org.apache.gravitino.utils.PasswordEncryptorCLI \
 *   encrypt "plaintext-password" --key "your-master-key"
 *
 * # Decrypt a password
 * java -cp libs/gravitino-common-*.jar org.apache.gravitino.utils.PasswordEncryptorCLI \
 *   decrypt "ENC(base64data)" --key "your-master-key"
 * </pre>
 */
public class PasswordEncryptorCLI {

  private PasswordEncryptorCLI() {}

  private static boolean isBlank(String str) {
    return str == null || str.trim().isEmpty();
  }

  /**
   * Main entry point for the password encryption/decryption CLI.
   *
   * @param args command line arguments
   */
  public static void main(String[] args) {
    if (args.length == 0) {
      printHelp();
      System.exit(1);
    }

    String action = args[0].toLowerCase();
    switch (action) {
      case "encrypt":
        runEncrypt(args);
        break;
      case "decrypt":
        runDecrypt(args);
        break;
      case "help":
      case "--help":
      case "-h":
        printHelp();
        break;
      default:
        System.err.println("Unknown action: " + action);
        printHelp();
        System.exit(1);
    }
  }

  /**
   * Handles the encrypt action. Reads the plaintext password from args or interactive prompt, and
   * the master key from args, env var, or system property.
   *
   * @param args command line arguments
   */
  private static void runEncrypt(String[] args) {
    String plaintext = null;
    String masterKey = null;

    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("--key")) {
        if (i + 1 >= args.length) {
          System.err.println("Error: --key requires a value");
          System.exit(1);
        }
        masterKey = args[++i];
      } else if (!args[i].startsWith("--")) {
        plaintext = args[i];
      }
    }

    if (isBlank(masterKey)) {
      masterKey = PasswordEncryptor.resolveMasterPassword();
    }

    if (PasswordEncryptor.DEFAULT_ENCRYPTION_KEY.equals(masterKey)) {
      System.err.println(
          "Warning: Using default master key \""
              + PasswordEncryptor.DEFAULT_ENCRYPTION_KEY
              + "\". Set a custom key via --key option or environment variable "
              + PasswordEncryptor.ENCRYPTION_KEY_ENV
              + " for production use.");
    }

    if (isBlank(plaintext)) {
      System.out.print("Enter password to encrypt: ");
      Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8.name());
      plaintext = scanner.nextLine();
    }

    if (isBlank(plaintext)) {
      System.err.println("Error: Password to encrypt must not be empty.");
      System.exit(1);
    }

    String encrypted = PasswordEncryptor.encrypt(plaintext, masterKey);
    System.out.println(encrypted);
  }

  /**
   * Handles the decrypt action. Reads the encrypted value from args or interactive prompt, and the
   * master key from args, env var, or system property.
   *
   * @param args command line arguments
   */
  private static void runDecrypt(String[] args) {
    String encryptedValue = null;
    String masterKey = null;

    for (int i = 1; i < args.length; i++) {
      if (args[i].equals("--key")) {
        if (i + 1 >= args.length) {
          System.err.println("Error: --key requires a value");
          System.exit(1);
        }
        masterKey = args[++i];
      } else if (!args[i].startsWith("--")) {
        encryptedValue = args[i];
      }
    }

    if (isBlank(masterKey)) {
      masterKey = PasswordEncryptor.resolveMasterPassword();
    }

    if (isBlank(encryptedValue)) {
      System.out.print("Enter encrypted value (ENC(...)): ");
      Scanner scanner = new Scanner(System.in, StandardCharsets.UTF_8.name());
      encryptedValue = scanner.nextLine();
    }

    if (!PasswordEncryptor.isEncrypted(encryptedValue)) {
      System.err.println("Error: Value is not in ENC(...) format.");
      System.exit(1);
    }

    try {
      String decrypted = PasswordEncryptor.decrypt(encryptedValue, masterKey);
      System.out.println(decrypted);
    } catch (Exception e) {
      System.err.println("Error: Failed to decrypt - " + e.getMessage());
      System.exit(1);
    }
  }

  /** Prints usage information. */
  private static void printHelp() {
    System.out.println("Gravitino Password Encryption Tool");
    System.out.println();
    System.out.println("Usage:");
    System.out.println(
        "  encrypt [password] [--key MASTER_KEY]   Encrypt a password to ENC(...) format");
    System.out.println(
        "  decrypt [ENC(...)] [--key MASTER_KEY]   Decrypt an ENC(...) formatted value");
    System.out.println("  help                                     Show this help message");
    System.out.println();
    System.out.println("Options:");
    System.out.println(
        "  --key MASTER_KEY   Master encryption key (if not provided, reads from "
            + "env var "
            + PasswordEncryptor.ENCRYPTION_KEY_ENV
            + ", system property "
            + PasswordEncryptor.ENCRYPTION_KEY_SYSTEM_PROPERTY
            + ", or defaults to \""
            + PasswordEncryptor.DEFAULT_ENCRYPTION_KEY
            + "\")");
    System.out.println();
    System.out.println("Examples:");
    System.out.println("  # Encrypt with master key from environment");
    System.out.println("  export " + PasswordEncryptor.ENCRYPTION_KEY_ENV + "=\"my-secret-key\"");
    System.out.println(
        "  java -cp libs/gravitino-common-*.jar "
            + PasswordEncryptorCLI.class.getName()
            + " encrypt \"myPassword\"");
    System.out.println();
    System.out.println("  # Encrypt with explicit master key");
    System.out.println(
        "  java -cp libs/gravitino-common-*.jar "
            + PasswordEncryptorCLI.class.getName()
            + " encrypt \"myPassword\" --key \"my-secret-key\"");
    System.out.println();
    System.out.println("  # Interactive mode (password will be prompted)");
    System.out.println(
        "  java -cp libs/gravitino-common-*.jar "
            + PasswordEncryptorCLI.class.getName()
            + " encrypt --key \"my-secret-key\"");
    System.out.println();
    System.out.println("  # Decrypt");
    System.out.println(
        "  java -cp libs/gravitino-common-*.jar "
            + PasswordEncryptorCLI.class.getName()
            + " decrypt \"ENC(base64data)\" --key \"my-secret-key\"");
  }
}
