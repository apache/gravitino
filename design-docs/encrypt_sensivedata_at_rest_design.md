# Encrypt sensitive data at Rest for Apache Gravitino

---

## Background

Apache Gravitino stores catalog configurations, metadata, and user information in a backend relational database (PostgreSQL, MySQL, or H2). Some of this data contains sensitive information that requires encryption at rest to meet security and compliance requirements.

### Security Risks Today

1. **Plain Text Sensitive Data in Database:**
   When catalog properties contain sensitive information, they are stored in plain text in Gravitino's backend database:
   ```sql
   -- catalog_meta_properties table
   catalog_id: 123
   property_name: "jdbc-password"
   property_value: "MyDatabasePassword123"  --  Plain text
   ```
   
2. **Database Compromise Risk:**
   - Database backups may be stored insecurely
   - Database administrators have unrestricted access to sensitive data
   - SQL injection vulnerabilities could expose sensitive data

### Relationship with External Secret Management

This design is **complementary** to the [External Secret Management Design](./EXTERNAL_SECRET_MANAGEMENT_DESIGN.md):

| Design | Purpose | When to Use | Data Flow |
|--------|---------|-------------|-----------|
| **SecretProvider** (External Secret Management) | Store references to secrets in external systems (AWS Secrets Manager, Vault) |  Need secret rotation<br> Need audit logging | `DB stores: ${secret:ref}` → Runtime resolves from external system |
| **EncryptionProvider** (This Design) | Encrypt sensitive data before storing in Gravitino's database |  Self-hosted without external dependencies<br> Need at-rest encryption<br> Simpler deployment | `DB stores: enc:AES-GCM:base64(encrypted_data)` → Runtime decrypts |

**Can be used together:**
```properties
# Option 1: External secret reference (preferred for production)
jdbc-password = ${secret:prod-db/password}

# Option 2: Encrypted value (fallback for self-hosted)
jdbc-password = enc:AES-GCM:dGVzdGRhdGE...

# Option 3: Plain text (backward compatible, not recommended)
jdbc-password = MyPlainPassword
```

### Integration: When Both Are Enabled

**Important:** Secret references (`${secret:...}`) are **NOT encrypted** because they're not sensitive - they're just pointers to secrets.

#### Processing Priority at Catalog Creation

```java
// User input: jdbc-password = "value"

// Priority 1: Is it a secret reference?
if (value.startsWith("${secret:")) {
  // Store as-is, DO NOT encrypt
  // Stored in DB: "${secret:prod-db/password}"
  // Reason: References are metadata, not secrets
  return value;
}

// Priority 2: Is it already encrypted?
if (value.startsWith("enc:")) {
  // Already encrypted, store as-is
  return value;
}

// Priority 3: Plain text - encrypt if enabled
if (encryptionEnabled) {
  // Encrypt before storing
  // Stored in DB: "enc:AES-GCM:v1:dGVzdA=="
  return encryptionProvider.encrypt(value);
}

// Priority 4: Plain text with no protection (backward compatibility)
return value;
```

#### Resolution Priority at Runtime

```java
// Value loaded from DB

// Priority 1: Secret reference?
if (value.startsWith("${secret:")) {
  // Resolve from external secret manager
  return secretProvider.resolve(value);
}

// Priority 2: Encrypted?
if (value.startsWith("enc:")) {
  // Decrypt
  return encryptionProvider.decrypt(value);
}

// Priority 3: Plain text
return value;
```

#### Why Not Encrypt Secret References?

**Secret references are NOT sensitive:**

| Value Type | Example | Is Sensitive? | Should Encrypt? |
|------------|---------|---------------|-----------------|
| Secret Reference | `${secret:prod-db/password}` |  No (just metadata) |  No |
| Actual Secret | `MyDatabasePassword123` |  Yes |  Yes |

**Encrypting a reference would be redundant:**

```
 BAD FLOW:
User: ${secret:db/password}
  ↓ encrypt (pointless)
DB: enc:AES-GCM:JHtzZWNyZXQ6ZGIvcGFzc3dvcmR9
  ↓ decrypt at runtime
${secret:db/password}
  ↓ resolve
MyActualPassword123

 GOOD FLOW:
User: ${secret:db/password}
  ↓ store as-is
DB: ${secret:db/password}
  ↓ resolve at runtime
MyActualPassword123
```

#### Configuration Matrix

| SecretProvider | EncryptionProvider | Plain Text Input | Secret Reference Input |
|----------------|-------------------|------------------|------------------------|
|  Disabled |  Disabled | Stored as plain text | Stored as-is (no resolution) |
|  Enabled |  Disabled | Stored as plain text | Stored as-is, resolved at runtime |
|  Disabled |  Enabled | Encrypted before storage | Stored as-is (treated as plain text, won't resolve) |
|  Enabled |  Enabled | Encrypted before storage | **Stored as-is, NOT encrypted**, resolved at runtime |

**Recommended Configuration:**
- **Production with external infrastructure**: Enable SecretProvider only
- **Self-hosted without external dependencies**: Enable EncryptionProvider only
- **Hybrid environment**: Enable both - secret references take priority

## Goals

1. **Encrypt sensitive catalog properties** before storing in Gravitino's backend database
2. **Pluggable encryption architecture** to support multiple encryption providers
3. **AES-GCM encryption** as the default implementation with secure key management
4. **Transparent encryption/decryption** - no changes to existing APIs
5. **Unique Initialization Vector (IV)** for each encrypted value
6. **Key management flexibility** - support env vars, K8s Secrets, and future KMS integration
7. **Backward compatibility** with existing plain-text catalog properties

## Design Details

### High-Level Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│  Gravitino API Layer                                              │
│  - Catalog create/update/read operations                          │
└──────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│  EncryptionService (Pluggable)                                    │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ + encrypt(plaintext): String                               │  │
│  │ + decrypt(ciphertext): String                              │  │
│  │ + isEncrypted(value): boolean                              │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
┌──────────────┐  ┌──────────────────┐  ┌─────────────────┐
│ AES-GCM      │  │  AWS KMS         │  │  Noop           │
│ Provider     │  │  Provider        │  │  (Plain Text)   │
│ (Default)    │  │  (Future)        │  │  (Backward      │
│              │  │                  │  │  Compatibility) │
└──────────────┘  └──────────────────┘  └─────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────────────────────┐
│  KeyProvider (Pluggable Key Source)                               │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ + getMasterKey(): SecretKey                                │  │
│  │ + rotateMasterKey(): void                                  │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
┌──────────────┐  ┌──────────────────┐  ┌─────────────────┐
│ Environment  │  │  Kubernetes      │  │  AWS KMS        │
│ Variable     │  │  Secret          │  │  (Future)       │
│ KeyProvider  │  │  KeyProvider     │  │                 │
└──────────────┘  └──────────────────┘  └─────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│  Gravitino Backend Database (PostgreSQL/MySQL)                    │
│                                                                   │
│  catalog_meta_properties:                                        │
│    property_name: "jdbc-password"                                │
│    property_value: "enc:AES-GCM:iv:base64data"  ← ENCRYPTED      │
│                                                                   │
│   Database compromise does NOT expose plain text secrets       │
└──────────────────────────────────────────────────────────────────┘
```

### Core Components

#### 1. EncryptionProvider Interface

```java
package org.apache.gravitino.encryption;

/**
 * An interface for encrypting and decrypting sensitive data before storing in database.
 * 
 * <p>Implementations provide different encryption mechanisms (AES-GCM, AWS KMS, etc.)
 * while maintaining a consistent interface for the catalog layer.
 * 
 * <p>Encrypted values use a standardized format:
 * enc:{algorithm}:{version}:{base64(iv:ciphertext:tag)}
 * 
 * Example: enc:AES-GCM:v1:dGVzdGRhdGE...
 */
public interface EncryptionProvider extends Closeable {
  
  /**
   * Encrypts plain text data.
   *
   * @param plaintext The plain text string to encrypt
   * @return Encrypted string in format: enc:{algorithm}:{version}:{data}
   * @throws EncryptionException if encryption fails
   */
  String encrypt(String plaintext) throws EncryptionException;
  
  /**
   * Decrypts cipher text data.
   *
   * @param ciphertext Encrypted string in format: enc:{algorithm}:{version}:{data}
   * @return Decrypted plain text string
   * @throws EncryptionException if decryption fails
   */
  String decrypt(String ciphertext) throws EncryptionException;
  
  /**
   * Checks if a value is encrypted.
   *
   * @param value The value to check
   * @return true if value starts with "enc:", false otherwise
   */
  default boolean isEncrypted(String value) {
    return value != null && value.startsWith("enc:");
  }
  
  /**
   * Returns the encryption algorithm identifier.
   *
   * @return Algorithm name (e.g., "AES-GCM", "AWS-KMS")
   */
  String getAlgorithm();
  
  /**
   * Returns the current encryption version.
   * Used for key rotation scenarios.
   *
   * @return Version identifier (e.g., "v1", "v2")
   */
  String getVersion();
  
  /**
   * Initializes the encryption provider with configuration properties.
   *
   * @param properties Configuration properties
   * @throws EncryptionException if initialization fails
   */
  void initialize(Map<String, String> properties) throws EncryptionException;
}
```

#### 2. AES-GCM Encryption Provider (Default Implementation)

```java
package org.apache.gravitino.encryption.aes;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * AES-GCM encryption provider implementation.
 * 
 * <p>Uses AES-256 in GCM mode with 96-bit IV and 128-bit authentication tag.
 * 
 * <p>Encrypted format:
 * enc:AES-GCM:v1:{base64(iv + ciphertext + tag)}
 * 
 * <p>Security properties:
 * - AES-256-GCM provides confidentiality and authenticity
 * - Unique IV per encryption prevents pattern analysis
 * - Authentication tag prevents tampering
 * - No padding oracle attacks (GCM is an authenticated mode)
 * 
 * <p>Configuration:
 * - encryption.aes.key-provider: KeyProvider implementation class
 * - encryption.aes.key-version: Key version for rotation (default: v1)
 */
public class AesGcmEncryptionProvider implements EncryptionProvider {
  
  private static final String ALGORITHM = "AES/GCM/NoPadding";
  private static final String PROVIDER_NAME = "AES-GCM";
  private static final int GCM_IV_LENGTH = 12; // 96 bits
  private static final int GCM_TAG_LENGTH = 16; // 128 bits
  
  private KeyProvider keyProvider;
  private SecureRandom secureRandom;
  private String keyVersion;
  
  @Override
  public void initialize(Map<String, String> properties) throws EncryptionException {
    try {
      // Initialize key provider
      String keyProviderClass = properties.getOrDefault(
          "encryption.aes.key-provider",
          "org.apache.gravitino.encryption.aes.EnvVarKeyProvider");
      
      this.keyProvider = (KeyProvider) Class.forName(keyProviderClass)
          .getDeclaredConstructor()
          .newInstance();
      this.keyProvider.initialize(properties);
      
      // Initialize secure random
      this.secureRandom = SecureRandom.getInstanceStrong();
      
      // Get key version for rotation support
      this.keyVersion = properties.getOrDefault("encryption.aes.key-version", "v1");
      
      LOG.info("AES-GCM encryption provider initialized with key version: {}", keyVersion);
    } catch (Exception e) {
      throw new EncryptionException("Failed to initialize AES-GCM provider", e);
    }
  }
  
  @Override
  public String encrypt(String plaintext) throws EncryptionException {
    if (plaintext == null || plaintext.isEmpty()) {
      return plaintext;
    }
    
    try {
      // Generate unique IV for this encryption
      byte[] iv = new byte[GCM_IV_LENGTH];
      secureRandom.nextBytes(iv);
      
      // Get encryption key
      SecretKey key = keyProvider.getMasterKey(keyVersion);
      
      // Initialize cipher
      Cipher cipher = Cipher.getInstance(ALGORITHM);
      GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, iv);
      cipher.init(Cipher.ENCRYPT_MODE, key, gcmParameterSpec);
      
      // Encrypt
      byte[] plaintextBytes = plaintext.getBytes(StandardCharsets.UTF_8);
      byte[] ciphertext = cipher.doFinal(plaintextBytes);
      
      // Combine IV + ciphertext + tag
      ByteBuffer byteBuffer = ByteBuffer.allocate(iv.length + ciphertext.length);
      byteBuffer.put(iv);
      byteBuffer.put(ciphertext);
      byte[] combined = byteBuffer.array();
      
      // Encode to base64
      String encoded = Base64.getEncoder().encodeToString(combined);
      
      // Return formatted encrypted string
      return String.format("enc:%s:%s:%s", PROVIDER_NAME, keyVersion, encoded);
      
    } catch (Exception e) {
      throw new EncryptionException("Failed to encrypt data", e);
    }
  }
  
  @Override
  public String decrypt(String ciphertext) throws EncryptionException {
    if (ciphertext == null || !isEncrypted(ciphertext)) {
      return ciphertext; // Return as-is if not encrypted (backward compatibility)
    }
    
    try {
      // Parse encrypted format: enc:AES-GCM:v1:base64data
      String[] parts = ciphertext.split(":", 4);
      if (parts.length != 4 || !parts[0].equals("enc") || !parts[1].equals(PROVIDER_NAME)) {
        throw new EncryptionException("Invalid encrypted value format");
      }
      
      String version = parts[2];
      String encoded = parts[3];
      
      // Decode from base64
      byte[] combined = Base64.getDecoder().decode(encoded);
      
      // Extract IV and ciphertext
      ByteBuffer byteBuffer = ByteBuffer.wrap(combined);
      byte[] iv = new byte[GCM_IV_LENGTH];
      byteBuffer.get(iv);
      
      byte[] ciphertextBytes = new byte[byteBuffer.remaining()];
      byteBuffer.get(ciphertextBytes);
      
      // Get decryption key
      SecretKey key = keyProvider.getMasterKey(version);
      
      // Initialize cipher
      Cipher cipher = Cipher.getInstance(ALGORITHM);
      GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH * 8, iv);
      cipher.init(Cipher.DECRYPT_MODE, key, gcmParameterSpec);
      
      // Decrypt
      byte[] plaintext = cipher.doFinal(ciphertextBytes);
      
      return new String(plaintext, StandardCharsets.UTF_8);
      
    } catch (Exception e) {
      throw new EncryptionException("Failed to decrypt data", e);
    }
  }
  
  @Override
  public String getAlgorithm() {
    return PROVIDER_NAME;
  }
  
  @Override
  public String getVersion() {
    return keyVersion;
  }
  
  @Override
  public void close() throws IOException {
    if (keyProvider != null) {
      keyProvider.close();
    }
  }
}
```

#### 3. KeyProvider Interface

```java
package org.apache.gravitino.encryption.aes;

import javax.crypto.SecretKey;

/**
 * Interface for providing encryption keys.
 * 
 * <p>Implementations can source keys from different locations:
 * - Environment variables
 * - Kubernetes Secrets
 * - AWS KMS
 * - HashiCorp Vault
 * - Local keystore files
 */
public interface KeyProvider extends Closeable {
  
  /**
   * Retrieves the master encryption key for the specified version.
   *
   * @param version Key version identifier (e.g., "v1", "v2")
   * @return SecretKey for encryption/decryption
   * @throws KeyProviderException if key retrieval fails
   */
  SecretKey getMasterKey(String version) throws KeyProviderException;
  
  /**
   * Initializes the key provider with configuration properties.
   *
   * @param properties Configuration properties
   * @throws KeyProviderException if initialization fails
   */
  void initialize(Map<String, String> properties) throws KeyProviderException;
  
  /**
   * Lists available key versions.
   *
   * @return Set of available key version identifiers
   */
  Set<String> listKeyVersions();
}
```

#### 4. Environment Variable KeyProvider (Default Implementation)

```java
package org.apache.gravitino.encryption.aes;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * KeyProvider that reads encryption keys from environment variables.
 * 
 * <p>Configuration:
 * - GRAVITINO_ENCRYPTION_KEY: Base64-encoded 256-bit AES key (required)
 * - GRAVITINO_ENCRYPTION_KEY_V2: Optional key for rotation scenarios
 * 
 * <p>Key generation example:
 * <pre>
 * # Generate a secure 256-bit key
 * openssl rand -base64 32
 * 
 * # Set as environment variable
 * export GRAVITINO_ENCRYPTION_KEY="your-base64-encoded-key"
 * </pre>
 * 
 * <p>Security considerations:
 * - Environment variables are visible to the process and child processes
 * - Better than hard-coding in config files
 * - Less secure than KMS/Vault but acceptable for self-hosted deployments
 * - Keys should be injected via Kubernetes Secrets in production
 */
public class EnvVarKeyProvider implements KeyProvider {
  
  private static final String DEFAULT_KEY_ENV = "GRAVITINO_ENCRYPTION_KEY";
  private static final int AES_KEY_SIZE = 32; // 256 bits
  
  private final Map<String, SecretKey> keyCache = new ConcurrentHashMap<>();
  
  @Override
  public void initialize(Map<String, String> properties) throws KeyProviderException {
    // Pre-load default key
    String defaultKeyEnv = properties.getOrDefault(
        "encryption.key.env-var-name", DEFAULT_KEY_ENV);
    
    String encodedKey = System.getenv(defaultKeyEnv);
    if (encodedKey == null || encodedKey.isEmpty()) {
      throw new KeyProviderException(
          "Encryption key not found in environment variable: " + defaultKeyEnv);
    }
    
    try {
      byte[] keyBytes = Base64.getDecoder().decode(encodedKey);
      if (keyBytes.length != AES_KEY_SIZE) {
        throw new KeyProviderException(
            "Invalid key size: " + keyBytes.length + " bytes. Expected " + AES_KEY_SIZE);
      }
      
      SecretKey key = new SecretKeySpec(keyBytes, "AES");
      keyCache.put("v1", key);
      
      LOG.info("Encryption key loaded from environment variable: {}", defaultKeyEnv);
    } catch (IllegalArgumentException e) {
      throw new KeyProviderException("Invalid base64-encoded key", e);
    }
    
    // Load additional versioned keys for rotation
    for (int version = 2; version <= 10; version++) {
      String versionedEnv = defaultKeyEnv + "_V" + version;
      String versionedKey = System.getenv(versionedEnv);
      if (versionedKey != null && !versionedKey.isEmpty()) {
        try {
          byte[] keyBytes = Base64.getDecoder().decode(versionedKey);
          SecretKey key = new SecretKeySpec(keyBytes, "AES");
          keyCache.put("v" + version, key);
          LOG.info("Loaded encryption key version: v{}", version);
        } catch (Exception e) {
          LOG.warn("Failed to load key version v{}: {}", version, e.getMessage());
        }
      }
    }
  }
  
  @Override
  public SecretKey getMasterKey(String version) throws KeyProviderException {
    SecretKey key = keyCache.get(version);
    if (key == null) {
      throw new KeyProviderException("Encryption key not found for version: " + version);
    }
    return key;
  }
  
  @Override
  public Set<String> listKeyVersions() {
    return keyCache.keySet();
  }
  
  @Override
  public void close() throws IOException {
    // Clear keys from memory
    keyCache.clear();
  }
}
```

#### 5. Kubernetes Secret KeyProvider

```java
package org.apache.gravitino.encryption.aes;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Secret;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * KeyProvider that reads encryption keys from Kubernetes Secrets.
 * 
 * <p>Configuration:
 * - encryption.k8s.secret-name: Name of the K8s Secret (default: gravitino-encryption-keys)
 * - encryption.k8s.secret-namespace: Namespace (default: default)
 * - encryption.k8s.key-field: Key field name in secret data (default: master-key)
 * 
 * <p>Kubernetes Secret example:
 * <pre>
 * apiVersion: v1
 * kind: Secret
 * metadata:
 *   name: gravitino-encryption-keys
 *   namespace: gravitino
 * type: Opaque
 * data:
 *   master-key: <base64-encoded-256-bit-key>
 *   master-key-v2: <base64-encoded-256-bit-key>  # For rotation
 * </pre>
 * 
 * <p>Security benefits over environment variables:
 * - Secrets can be encrypted at rest in etcd (if configured)
 * - RBAC controls access to secrets
 * - Easier rotation without pod restart (if using mounted secrets)
 * - Audit logging of secret access
 */
public class KubernetesSecretKeyProvider implements KeyProvider {
  
  private static final String DEFAULT_SECRET_NAME = "gravitino-encryption-keys";
  private static final String DEFAULT_KEY_FIELD = "master-key";
  
  private final Map<String, SecretKey> keyCache = new ConcurrentHashMap<>();
  private CoreV1Api api;
  private String secretName;
  private String namespace;
  
  @Override
  public void initialize(Map<String, String> properties) throws KeyProviderException {
    try {
      // Initialize Kubernetes API client
      ApiClient client = io.kubernetes.client.util.Config.defaultClient();
      this.api = new CoreV1Api(client);
      
      this.secretName = properties.getOrDefault(
          "encryption.k8s.secret-name", DEFAULT_SECRET_NAME);
      this.namespace = properties.getOrDefault(
          "encryption.k8s.secret-namespace", "default");
      
      String keyField = properties.getOrDefault(
          "encryption.k8s.key-field", DEFAULT_KEY_FIELD);
      
      // Load secret from Kubernetes
      V1Secret secret = api.readNamespacedSecret(secretName, namespace, null);
      
      if (secret.getData() == null) {
        throw new KeyProviderException("Secret has no data: " + secretName);
      }
      
      // Load master key (v1)
      byte[] keyBytes = secret.getData().get(keyField);
      if (keyBytes == null || keyBytes.length != 32) {
        throw new KeyProviderException("Invalid or missing master key in secret");
      }
      
      SecretKey key = new SecretKeySpec(keyBytes, "AES");
      keyCache.put("v1", key);
      
      // Load versioned keys for rotation
      for (int version = 2; version <= 10; version++) {
        String versionedField = keyField + "-v" + version;
        byte[] versionedKeyBytes = secret.getData().get(versionedField);
        if (versionedKeyBytes != null && versionedKeyBytes.length == 32) {
          SecretKey versionedKey = new SecretKeySpec(versionedKeyBytes, "AES");
          keyCache.put("v" + version, key);
          LOG.info("Loaded encryption key version: v{}", version);
        }
      }
      
      LOG.info("Encryption keys loaded from Kubernetes Secret: {}/{}", namespace, secretName);
      
    } catch (Exception e) {
      throw new KeyProviderException("Failed to load keys from Kubernetes Secret", e);
    }
  }
  
  @Override
  public SecretKey getMasterKey(String version) throws KeyProviderException {
    SecretKey key = keyCache.get(version);
    if (key == null) {
      throw new KeyProviderException("Encryption key not found for version: " + version);
    }
    return key;
  }
  
  @Override
  public Set<String> listKeyVersions() {
    return keyCache.keySet();
  }
  
  @Override
  public void close() throws IOException {
    keyCache.clear();
  }
}
```

#### 6. EncryptionService (Integration Layer)

```java
package org.apache.gravitino.catalog;

import org.apache.gravitino.encryption.EncryptionProvider;
import org.apache.gravitino.encryption.aes.AesGcmEncryptionProvider;

/**
 * Service for encrypting/decrypting catalog properties.
 * 
 * <p>Integrates with the catalog persistence layer to automatically
 * encrypt sensitive properties before storage and decrypt on retrieval.
 * 
 * <p>Sensitive properties that should be encrypted:
 * - jdbc-password
 * - jdbc-user (if contains sensitive info)
 * - s3-secret-access-key
 * - s3-access-key-id
 * - authentication.kerberos.keytab-uri
 * - Any property ending with "-password", "-secret", "-key"
 */
public class EncryptionService {
  
  private static final Set<String> SENSITIVE_PROPERTY_NAMES = ImmutableSet.of(
      "jdbc-password",
      "s3-secret-access-key",
      "s3-access-key-id",
      "oss-secret-access-key",
      "azure-storage-account-key",
      "azure-client-secret",
      "authentication.kerberos.keytab-uri"
  );
  
  private static final Pattern SENSITIVE_PROPERTY_PATTERN = 
      Pattern.compile(".*(-password|-secret|-key|-token)$", Pattern.CASE_INSENSITIVE);
  
  private EncryptionProvider encryptionProvider;
  private boolean encryptionEnabled;
  
  /**
   * Initializes the encryption service.
   *
   * @param config Gravitino configuration
   */
  public void initialize(Config config) {
    this.encryptionEnabled = config.get("gravitino.encryption.enabled", false);
    
    if (!encryptionEnabled) {
      LOG.info("Encryption at rest is disabled");
      return;
    }
    
    String providerClass = config.get(
        "gravitino.encryption.provider",
        "org.apache.gravitino.encryption.aes.AesGcmEncryptionProvider");
    
    try {
      this.encryptionProvider = (EncryptionProvider) Class.forName(providerClass)
          .getDeclaredConstructor()
          .newInstance();
      
      // Convert Config to Map for provider initialization
      Map<String, String> properties = config.getAllConfig();
      this.encryptionProvider.initialize(properties);
      
      LOG.info("Encryption at rest enabled with provider: {}", providerClass);
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize encryption provider", e);
    }
  }
  
  /**
   * Encrypts catalog properties before storage.
   *
   * @param properties Catalog properties map
   * @return Map with sensitive values encrypted
   */
  public Map<String, String> encryptProperties(Map<String, String> properties) {
    if (!encryptionEnabled || properties == null) {
      return properties;
    }
    
    Map<String, String> encrypted = new HashMap<>(properties);
    
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      
      if (isSensitiveProperty(key) && !encryptionProvider.isEncrypted(value)) {
        try {
          String encryptedValue = encryptionProvider.encrypt(value);
          encrypted.put(key, encryptedValue);
          LOG.debug("Encrypted property: {}", key);
        } catch (EncryptionException e) {
          LOG.error("Failed to encrypt property: {}", key, e);
          throw new RuntimeException("Encryption failed for property: " + key, e);
        }
      }
    }
    
    return encrypted;
  }
  
  /**
   * Decrypts catalog properties after retrieval.
   *
   * @param properties Catalog properties map (may contain encrypted values)
   * @return Map with encrypted values decrypted
   */
  public Map<String, String> decryptProperties(Map<String, String> properties) {
    if (!encryptionEnabled || properties == null) {
      return properties;
    }
    
    Map<String, String> decrypted = new HashMap<>(properties);
    
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      
      if (encryptionProvider.isEncrypted(value)) {
        try {
          String decryptedValue = encryptionProvider.decrypt(value);
          decrypted.put(key, decryptedValue);
          LOG.debug("Decrypted property: {}", key);
        } catch (EncryptionException e) {
          LOG.error("Failed to decrypt property: {}", key, e);
          throw new RuntimeException("Decryption failed for property: " + key, e);
        }
      }
    }
    
    return decrypted;
  }
  
  /**
   * Determines if a property should be encrypted.
   */
  private boolean isSensitiveProperty(String propertyName) {
    // Check explicit sensitive property names
    if (SENSITIVE_PROPERTY_NAMES.contains(propertyName)) {
      return true;
    }
    
    // Check pattern matching (ends with -password, -secret, -key, -token)
    return SENSITIVE_PROPERTY_PATTERN.matcher(propertyName).matches();
  }
  
  /**
   * Encrypts a single value (utility method).
   */
  public String encryptValue(String plaintext) {
    if (!encryptionEnabled || plaintext == null) {
      return plaintext;
    }
    
    try {
      return encryptionProvider.encrypt(plaintext);
    } catch (EncryptionException e) {
      throw new RuntimeException("Failed to encrypt value", e);
    }
  }
  
  /**
   * Decrypts a single value (utility method).
   */
  public String decryptValue(String ciphertext) {
    if (!encryptionEnabled || ciphertext == null) {
      return ciphertext;
    }
    
    try {
      return encryptionProvider.decrypt(ciphertext);
    } catch (EncryptionException e) {
      throw new RuntimeException("Failed to decrypt value", e);
    }
  }
}
```

### Service Discovery

Use Java ServiceLoader for provider discovery:

```
META-INF/services/org.apache.gravitino.encryption.EncryptionProvider
```

Content:
```
org.apache.gravitino.encryption.aes.AesGcmEncryptionProvider
org.apache.gravitino.encryption.noop.NoopEncryptionProvider
```

## Configuration

### Server Configuration (`gravitino.conf`)

```properties
# Enable encryption at rest
gravitino.encryption.enabled = true

# Encryption provider (default: AES-GCM)
gravitino.encryption.provider = org.apache.gravitino.encryption.aes.AesGcmEncryptionProvider

# AES-GCM specific configuration
# Key provider: where to load encryption keys from
encryption.aes.key-provider = org.apache.gravitino.encryption.aes.EnvVarKeyProvider
# encryption.aes.key-provider = org.apache.gravitino.encryption.aes.KubernetesSecretKeyProvider

# Key version for new encryptions (for rotation)
encryption.aes.key-version = v1

# Environment variable key provider configuration
encryption.key.env-var-name = GRAVITINO_ENCRYPTION_KEY

# Kubernetes secret key provider configuration (if using K8s)
# encryption.k8s.secret-name = gravitino-encryption-keys
# encryption.k8s.secret-namespace = gravitino
# encryption.k8s.key-field = master-key
```

### Generating Encryption Keys

```bash
# Generate a secure 256-bit AES key
openssl rand -base64 32

# Output example:
# 3q2+7w5e8r9t0y1u2i3o4p5a6s7d8f9g0h1j2k3l4z5x=

# Set as environment variable
export GRAVITINO_ENCRYPTION_KEY="3q2+7w5e8r9t0y1u2i3o4p5a6s7d8f9g0h1j2k3l4z5x="

# For Kubernetes, create a secret
kubectl create secret generic gravitino-encryption-keys \
  --from-literal=master-key=$(openssl rand -base64 32) \
  --namespace gravitino
```

### Kubernetes Deployment with Encryption

**Option 1: Environment Variable from Secret**

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: gravitino-encryption-key
  namespace: gravitino
type: Opaque
data:
  key: M3EyKzd3NWU4cjl0MHkxdTJpM280cDVhNnM3ZDhmOWcwaDF==  # base64(base64-key)

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gravitino
spec:
  template:
    spec:
      containers:
      - name: gravitino
        image: apache/gravitino:latest
        env:
        - name: GRAVITINO_ENCRYPTION_KEY
          valueFrom:
            secretKeyRef:
              name: gravitino-encryption-key
              key: key
        volumeMounts:
        - name: config
          mountPath: /gravitino/conf/gravitino.conf
          subPath: gravitino.conf
```

**Option 2: Kubernetes Secret KeyProvider (Recommended)**

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: gravitino-encryption-keys
  namespace: gravitino
type: Opaque
data:
  master-key: M3EyKzd3NWU4cjl0MHkxdTJpM280cDVhNnM3ZDhmOWcwaDE=  # 256-bit key
  master-key-v2: OXQ4eTd1NmkzcjJlMXcwcTl5OHQ3dTZpNXI0ZTN3Mg==     # For rotation

---
apiVersion: v1
kind: ConfigMap
metadata:
  name: gravitino-config
  namespace: gravitino
data:
  gravitino.conf: |
    gravitino.encryption.enabled = true
    gravitino.encryption.provider = org.apache.gravitino.encryption.aes.AesGcmEncryptionProvider
    encryption.aes.key-provider = org.apache.gravitino.encryption.aes.KubernetesSecretKeyProvider
    encryption.k8s.secret-name = gravitino-encryption-keys
    encryption.k8s.secret-namespace = gravitino
    encryption.k8s.key-field = master-key
    encryption.aes.key-version = v1

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gravitino
spec:
  template:
    spec:
      serviceAccountName: gravitino
      containers:
      - name: gravitino
        image: apache/gravitino:latest
        volumeMounts:
        - name: config
          mountPath: /gravitino/conf/gravitino.conf
          subPath: gravitino.conf
      volumes:
      - name: config
        configMap:
          name: gravitino-config

---
# ServiceAccount with RBAC to read secrets
apiVersion: v1
kind: ServiceAccount
metadata:
  name: gravitino
  namespace: gravitino

---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: gravitino-secret-reader
  namespace: gravitino
rules:
- apiGroups: [""]
  resources: ["secrets"]
  resourceNames: ["gravitino-encryption-keys"]
  verbs: ["get"]

---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: gravitino-secret-reader-binding
  namespace: gravitino
subjects:
- kind: ServiceAccount
  name: gravitino
  namespace: gravitino
roleRef:
  kind: Role
  name: gravitino-secret-reader
  apiGroup: rbac.authorization.k8s.io
```

## Usage Examples

### Example 1: Create Catalog with Encryption Enabled

**User creates catalog via API:**

```bash
curl -X POST -H "Content-Type: application/json" \
  -d '{
    "name": "prod-postgres",
    "type": "RELATIONAL",
    "provider": "jdbc-postgresql",
    "properties": {
      "jdbc-url": "jdbc:postgresql://db.example.com:5432/prod",
      "jdbc-user": "app_user",
      "jdbc-password": "MyDatabasePassword123"
    }
  }' \
  http://localhost:8090/api/metalakes/my_metalake/catalogs
```

**What happens internally:**

```java
// 1. API receives catalog creation request
Map<String, String> properties = {
  "jdbc-url": "jdbc:postgresql://db.example.com:5432/prod",
  "jdbc-user": "app_user",
  "jdbc-password": "MyDatabasePassword123"  // Plain text
};

// 2. EncryptionService encrypts sensitive properties
properties = encryptionService.encryptProperties(properties);
// Result:
// {
//   "jdbc-url": "jdbc:postgresql://db.example.com:5432/prod",
//   "jdbc-user": "app_user",
//   "jdbc-password": "enc:AES-GCM:v1:dGVzdGRhdGE..."  // Encrypted
// }

// 3. Store encrypted properties in database
catalogStore.save(catalogName, properties);
```

**Database storage:**

```sql
-- catalog_meta_properties table
INSERT INTO catalog_meta_properties (catalog_id, property_name, property_value)
VALUES 
  (123, 'jdbc-url', 'jdbc:postgresql://db.example.com:5432/prod'),
  (123, 'jdbc-user', 'app_user'),
  (123, 'jdbc-password', 'enc:AES-GCM:v1:MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI=');
  --  Password encrypted, not plain text
```

### Example 2: Load Catalog and Connect to Database

```java
// 1. Load catalog from database
Map<String, String> encryptedProperties = catalogStore.load(catalogName);
// {
//   "jdbc-url": "jdbc:postgresql://db.example.com:5432/prod",
//   "jdbc-user": "app_user",
//   "jdbc-password": "enc:AES-GCM:v1:dGVzdGRhdGE..."
// }

// 2. Decrypt properties
Map<String, String> decryptedProperties = 
    encryptionService.decryptProperties(encryptedProperties);
// {
//   "jdbc-url": "jdbc:postgresql://db.example.com:5432/prod",
//   "jdbc-user": "app_user",
//   "jdbc-password": "MyDatabasePassword123"  // Decrypted
// }

// 3. Initialize catalog with decrypted properties
catalog.initialize(decryptedProperties);

// 4. Connect to PostgreSQL
Connection conn = DriverManager.getConnection(
    decryptedProperties.get("jdbc-url"),
    decryptedProperties.get("jdbc-user"),
    decryptedProperties.get("jdbc-password")
);
```

### Example 3: API Returns Masked Values

```bash
# GET catalog via API
curl http://localhost:8090/api/metalakes/my_metalake/catalogs/prod-postgres

# Response (sensitive values masked in API responses)
{
  "name": "prod-postgres",
  "type": "RELATIONAL",
  "provider": "jdbc-postgresql",
  "properties": {
    "jdbc-url": "jdbc:postgresql://db.example.com:5432/prod",
    "jdbc-user": "app_user",
    "jdbc-password": "***"  // Masked, not exposed
  }
}
```

### Example 4: Both SecretProvider and EncryptionProvider Enabled

**Configuration:**

```properties
# gravitino.conf - Both systems enabled

# External secret management
secret-provider = aws-secrets-manager
secret-provider.region = us-east-1

# Encryption at rest
gravitino.encryption.enabled = true
encryption.aes.key-provider = KubernetesSecretKeyProvider
```

**Scenario 1: User creates catalog with secret reference**

```bash
# User creates catalog
curl -X POST -H "Content-Type: application/json" \
  -d '{
    "name": "prod-postgres",
    "properties": {
      "jdbc-url": "jdbc:postgresql://db.example.com:5432/prod",
      "jdbc-password": "${secret:prod-db/password}"
    }
  }' \
  http://localhost:8090/api/metalakes/my_metalake/catalogs
```

**What happens:**

```java
// Input from API
jdbc-password = "${secret:prod-db/password}"

// Step 1: Check if secret reference
if (value.startsWith("${secret:")) {
  // YES - Store as-is, DO NOT encrypt
  storeInDatabase("jdbc-password", "${secret:prod-db/password}");
}
```

**Database storage:**

```sql
-- catalog_meta_properties table
property_name: "jdbc-password"
property_value: "${secret:prod-db/password}"  -- NOT encrypted
```

**At runtime (catalog initialization):**

```java
// Load from database
String value = loadFromDatabase("jdbc-password");
// value = "${secret:prod-db/password}"

// Step 1: Check if secret reference
if (value.startsWith("${secret:")) {
  // Resolve from AWS Secrets Manager
  String resolvedPassword = secretProvider.resolve(value);
  // resolvedPassword = "MyActualPassword123"
  return resolvedPassword;
}

// Step 2: Connect to PostgreSQL
Connection conn = DriverManager.getConnection(url, user, resolvedPassword);
// Uses: "MyActualPassword123"
```

**Scenario 2: User creates catalog with plain-text password**

```bash
# User creates catalog with plain text
curl -X POST -H "Content-Type: application/json" \
  -d '{
    "name": "dev-postgres",
    "properties": {
      "jdbc-url": "jdbc:postgresql://localhost:5432/dev",
      "jdbc-password": "MyPlainPassword123"
    }
  }' \
  http://localhost:8090/api/metalakes/my_metalake/catalogs
```

**What happens:**

```java
// Input from API
jdbc-password = "MyPlainPassword123"

// Step 1: Check if secret reference
if (value.startsWith("${secret:")) {
  // NO
}

// Step 2: Check if already encrypted
if (value.startsWith("enc:")) {
  // NO
}

// Step 3: Encryption enabled - encrypt it
if (encryptionEnabled) {
  String encrypted = encryptionProvider.encrypt("MyPlainPassword123");
  // encrypted = "enc:AES-GCM:v1:MTIzNDU2Nzg5MDEy..."
  storeInDatabase("jdbc-password", encrypted);
}
```

**Database storage:**

```sql
-- catalog_meta_properties table
property_name: "jdbc-password"
property_value: "enc:AES-GCM:v1:MTIzNDU2Nzg5MDEy..."  -- Encrypted
```

**At runtime:**

```java
// Load from database
String value = loadFromDatabase("jdbc-password");
// value = "enc:AES-GCM:v1:MTIzNDU2Nzg5MDEy..."

// Step 1: Check if secret reference
if (value.startsWith("${secret:")) {
  // NO
}

// Step 2: Check if encrypted
if (value.startsWith("enc:")) {
  // YES - Decrypt it
  String decrypted = encryptionProvider.decrypt(value);
  // decrypted = "MyPlainPassword123"
  return decrypted;
}

// Step 3: Connect to PostgreSQL
Connection conn = DriverManager.getConnection(url, user, decrypted);
// Uses: "MyPlainPassword123"
```

**Summary Table:**

| User Input | Stored in Database | Runtime Resolution | Final Value |
|------------|-------------------|-------------------|-------------|
| `${secret:prod-db/password}` | `${secret:prod-db/password}` (NOT encrypted) | Resolve from AWS Secrets Manager | `MyActualPassword123` |
| `MyPlainPassword123` | `enc:AES-GCM:v1:MTI...` (encrypted) | Decrypt with AES key | `MyPlainPassword123` |
| `enc:AES-GCM:v1:...` | `enc:AES-GCM:v1:...` (as-is) | Decrypt with AES key | Original plain text |

**Key Insight:** Secret references take priority and are **never encrypted** because:
-  They're not sensitive (just metadata)
-  Encrypting them would be redundant
-  External secret managers provide better security (rotation, audit, IAM)

## Security Considerations

### 1. Encryption Algorithm Choice

**Why AES-GCM?**
-  **Authenticated Encryption**: Provides both confidentiality and authenticity
-  **Unique IV per encryption**: Prevents pattern analysis

**Security Parameters:**
- Key size: 256 bits (AES-256)
- IV size: 96 bits (12 bytes) - recommended for GCM
- Authentication tag: 128 bits (16 bytes)

### 2. Key Management

**Key Storage Hierarchy (Best to Worst):**

1. **Kubernetes Secrets** (Recommended for now)
   -  RBAC-controlled access
   - ⚠️ Encrypted at rest (if etcd encryption enabled)
   -  Easier rotation than env vars

2. **Environment Variables**
   -  Better than config files
   - ⚠️ Visible in process listings
   - ⚠️ Difficult to rotate
   
3. **AWS KMS / Cloud Provider KMS** (Future)
   -  Hardware Security Modules (HSMs)
   -  Automatic key rotation
   -  Audit logging
   -  Fine-grained IAM policies

4. **HashiCorp Vault** (Future)
   -  Centralized key management
   -  Dynamic secrets
   -  Encryption as a Service
   
### 3. Key Rotation Strategy

```properties
# Step 1: Add new key version
export GRAVITINO_ENCRYPTION_KEY_V2="new-base64-key"

# Step 2: Update config to use new key for NEW encryptions
encryption.aes.key-version = v2

# Step 3: Old data still decrypts with v1 key
# enc:AES-GCM:v1:... uses v1 key
# enc:AES-GCM:v2:... uses v2 key

# Step 4: Re-encrypt old data (migration CLI tool)
gravitino-admin.sh encrypt-migrate --from v1 --to v2

# Step 5: Remove old key after migration complete
unset GRAVITINO_ENCRYPTION_KEY_V1
```

### 4. Threat Model

**What this design protects against:**

| Threat | Protection |
|--------|-----------|
| **Database backup theft** | Encrypted values unreadable without key |
| **SQL injection** |  Even if data extracted, it's encrypted |
| **Insider threat (DBA)** |  DBA cannot read encrypted passwords |
| **Physical disk theft** |  Encrypted data at rest |
| **Accidental logging** |  Encrypted values safe to log |

**What this design does NOT protect against:**

| Threat | Why Not Protected |
|--------|-------------------|
| **Gravitino server compromise** |  Server has decryption key in memory |
| **Application vulnerabilities** |  Decryption happens in application |
| **Key theft** |  If encryption key is stolen, game over |

### 5. Error Handling

```java
// Fail fast on startup if encryption misconfigured
@PostConstruct
public void validateEncryption() {
  if (encryptionEnabled) {
    try {
      // Test encrypt/decrypt
      String testValue = "test-encryption-" + UUID.randomUUID();
      String encrypted = encryptionProvider.encrypt(testValue);
      String decrypted = encryptionProvider.decrypt(encrypted);
      
      if (!testValue.equals(decrypted)) {
        throw new RuntimeException("Encryption validation failed");
      }
      
      LOG.info("Encryption validation successful");
    } catch (Exception e) {
      LOG.error("Encryption validation failed - shutting down", e);
      throw new RuntimeException("Encryption misconfigured", e);
    }
  }
}
```

## Migration Path

### Phase 1: Opt-In 

**Encryption is optional:**
- Default: `gravitino.encryption.enabled = false`
- Backward compatible with existing plain-text catalogs
- New catalogs can use encryption
- Existing catalogs remain unencrypted

### Phase 2: Migration Tool 

**Provide CLI tool to encrypt existing catalogs:**

```bash
# Encrypt all catalogs in a metalake
gravitino-admin.sh encrypt-catalogs \
  --metalake my_metalake \
  --dry-run  # Preview without making changes

# Actually encrypt
gravitino-admin.sh encrypt-catalogs \
  --metalake my_metalake \
  --backup /tmp/catalog-backup.sql

# Encrypt specific catalog
gravitino-admin.sh encrypt-catalog \
  --metalake my_metalake \
  --catalog prod-postgres
```

**Migration process:**

1. Enable encryption in config: `gravitino.encryption.enabled = true`
2. Restart Gravitino (no impact on existing catalogs)
3. Run migration tool: `encrypt-catalogs`
4. Tool reads plain-text properties, encrypts them, updates database
5. Verify all catalogs still work
6. Delete backup after verification

## Testing Strategy

### Unit Tests

1. **AesGcmEncryptionProvider Tests**
   ```java
   @Test
   public void testEncryptDecrypt() {
     String plaintext = "MySecretPassword123!";
     String encrypted = provider.encrypt(plaintext);
     String decrypted = provider.decrypt(encrypted);
     assertEquals(plaintext, decrypted);
   }
   
   @Test
   public void testUniqueIVs() {
     String plaintext = "password";
     String encrypted1 = provider.encrypt(plaintext);
     String encrypted2 = provider.encrypt(plaintext);
     assertNotEquals(encrypted1, encrypted2); // Different IVs
   }
   
   @Test
   public void testEncryptedFormat() {
     String encrypted = provider.encrypt("test");
     assertTrue(encrypted.startsWith("enc:AES-GCM:v1:"));
   }
   
   @Test
   public void testTampering() {
     String encrypted = provider.encrypt("password");
     String tampered = encrypted.substring(0, encrypted.length() - 5) + "XXXXX";
     assertThrows(EncryptionException.class, () -> provider.decrypt(tampered));
   }
   ```

2. **KeyProvider Tests**
   ```java
   @Test
   public void testEnvVarKeyProvider() {
     System.setenv("GRAVITINO_ENCRYPTION_KEY", generateBase64Key());
     KeyProvider provider = new EnvVarKeyProvider();
     SecretKey key = provider.getMasterKey("v1");
     assertNotNull(key);
     assertEquals("AES", key.getAlgorithm());
   }
   
   @Test
   public void testKeyRotation() {
     // Load v1 and v2 keys
     SecretKey v1 = provider.getMasterKey("v1");
     SecretKey v2 = provider.getMasterKey("v2");
     assertNotEquals(v1, v2);
   }
   ```

3. **EncryptionService Tests**
   ```java
   @Test
   public void testEncryptSensitiveProperties() {
     Map<String, String> props = Map.of(
       "jdbc-url", "jdbc:postgresql://localhost/db",
       "jdbc-password", "secret123"
     );
     
     Map<String, String> encrypted = service.encryptProperties(props);
     
     assertEquals(props.get("jdbc-url"), encrypted.get("jdbc-url"));
     assertTrue(encrypted.get("jdbc-password").startsWith("enc:"));
   }
   
   @Test
   public void testBackwardCompatibility() {
     Map<String, String> props = Map.of(
       "jdbc-password", "plain-text-password"
     );
     
     // Decrypt should return plain text as-is
     Map<String, String> decrypted = service.decryptProperties(props);
     assertEquals("plain-text-password", decrypted.get("jdbc-password"));
   }
   ```

### Integration Tests

1. **End-to-End Catalog Encryption**
   ```java
   @Test
   @Tag("gravitino-docker-test")
   public void testCatalogCreateWithEncryption() {
     // Enable encryption
     config.set("gravitino.encryption.enabled", true);
     
     // Create catalog
     Catalog catalog = client.createCatalog(
       "test-pg",
       Type.RELATIONAL,
       "jdbc-postgresql",
       Map.of(
         "jdbc-url", "jdbc:postgresql://localhost:5432/test",
         "jdbc-password", "mypassword"
       )
     );
     
     // Verify password encrypted in database
     String storedPassword = jdbcTemplate.queryForObject(
       "SELECT property_value FROM catalog_meta_properties " +
       "WHERE catalog_id = ? AND property_name = 'jdbc-password'",
       String.class,
       catalog.id()
     );
     
     assertTrue(storedPassword.startsWith("enc:AES-GCM:"));
     
     // Verify catalog still works
     catalog.asSchemas().listSchemas();
   }
   ```

2. **Migration Tool Test**
   ```java
   @Test
   public void testMigrationFromPlainText() {
     // Create catalog with plain text
     config.set("gravitino.encryption.enabled", false);
     createCatalog("test", Map.of("jdbc-password", "plain"));
     
     // Enable encryption
     config.set("gravitino.encryption.enabled", true);
     
     // Run migration
     MigrationTool.migrateCatalogs("metalake");
     
     // Verify encrypted
     String stored = getStoredPassword("test");
     assertTrue(stored.startsWith("enc:"));
     
     // Verify still works
     Catalog catalog = client.loadCatalog("test");
     assertNotNull(catalog);
   }
   ```

### Security Tests

1. **Key Validation**
   ```java
   @Test
   public void testInvalidKeySizeRejected() {
     // 128-bit key (too small)
     System.setenv("GRAVITINO_ENCRYPTION_KEY", 
       Base64.getEncoder().encodeToString(new byte[16]));
     
     assertThrows(KeyProviderException.class, 
       () -> new EnvVarKeyProvider().initialize(Map.of()));
   }
   ```

2. **Encryption Strength**
   ```java
   @Test
   public void testNoPatternLeakage() {
     // Encrypt same plaintext 1000 times
     Set<String> ciphertexts = new HashSet<>();
     for (int i = 0; i < 1000; i++) {
       ciphertexts.add(provider.encrypt("password"));
     }
     
     // All should be unique (different IVs)
     assertEquals(1000, ciphertexts.size());
   }
   ```

## Future Work

### Phase 2: KMS Integration

**AWS KMS Provider:**

```properties
gravitino.encryption.provider = org.apache.gravitino.encryption.aws.AwsKmsEncryptionProvider
encryption.aws.kms.key-id = arn:aws:kms:us-east-1:123456789012:key/abc123
encryption.aws.kms.region = us-east-1
```

### Phase 3: HashiCorp Vault Integration

### Phase 4: Field-Level Encryption

## Backward Compatibility

**100% backward compatible:**

1. **Encryption disabled by default**: Existing deployments continue working
2. **Mixed mode**: Can have both encrypted and plain-text catalogs
3. **Graceful degradation**: If decryption fails, catalog becomes unavailable (fail-safe)
4. **API unchanged**: No changes to public APIs

## Security Checklist

Before deploying encryption at rest:

- [ ] Generate strong 256-bit encryption key
- [ ] Store key in Kubernetes Secret (not environment variable)
- [ ] Enable Kubernetes etcd encryption at rest
- [ ] Configure RBAC for secret access
- [ ] Test encrypt/decrypt end-to-end
- [ ] Verify backup/restore works with encrypted data
- [ ] Document key rotation procedure
- [ ] Set up monitoring for encryption failures
- [ ] Plan migration path for existing catalogs

## Summary

### V1 Scope

 **Pluggable encryption architecture** with AES-GCM default implementation  
 **Transparent encryption/decryption** - no API changes required  
 **Multiple key providers** - Environment variables and Kubernetes Secrets  
 **Key versioning** support for rotation scenarios  
 **Backward compatibility** with plain-text catalogs  
 **Migration tool** to encrypt existing catalogs  
 **Comprehensive testing** with security validation  

### V2 and Beyond

 **KMS integration** (AWS KMS, Google Cloud KMS, Azure Key Vault)  
 **Vault Transit Encryption** for centralized key management  
 **Envelope encryption** for large values  
 **Field-level encryption** for user/role tables  
 **Automatic key rotation** with zero downtime  

**Key Takeaway:** This design provides encryption at rest for sensitive catalog properties while maintaining backward compatibility and allowing future integration with enterprise key management systems.

---

## References

1. [NIST Special Publication 800-38D: GCM Mode](https://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication800-38d.pdf)
2. [OWASP Cryptographic Storage Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Cryptographic_Storage_Cheat_Sheet.html)
3. [AWS Encryption SDK](https://docs.aws.amazon.com/encryption-sdk/latest/developer-guide/introduction.html)
4. [Kubernetes Secrets Encryption at Rest](https://kubernetes.io/docs/tasks/administer-cluster/encrypt-data/)
5. [External Secret Management Design](./EXTERNAL_SECRET_MANAGEMENT_DESIGN.md)
