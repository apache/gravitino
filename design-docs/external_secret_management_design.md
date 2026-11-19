# External Secret Management Integration for Apache Gravitino

---


## Background

Apache Gravitino currently supports credential vending for cloud storage systems (S3, GCS, ADLS, OSS) through various credential providers. However, sensitive credentials (access keys, secret keys, passwords, API tokens) are currently configured directly in configuration files or catalog properties as plain text, which poses significant security risks in production environments.

Modern cloud-native applications follow the principle of **never storing secrets in database as plain-text or in  configuration files** and instead rely on external secret management systems that provide:
- Centralized secret storage with encryption at rest
- Fine-grained access control and audit logging
- Automatic secret rotation
- Secret versioning and rollback capabilities
- Integration with cloud IAM systems

### Security Risks  Today

1. **Plain Text Catalog Credentials in Database:**
   When users create catalogs via UI/API, passwords are stored as plain text:
   ```properties
   # PostgreSQL Catalog
   jdbc-password = MyDatabasePassword123
   
   # MySQL Catalog  
   jdbc-password = SuperSecret456
   
   # Hive Catalog
   authentication.kerberos.keytab-uri = /path/to/keytab
   ```
   These credentials are stored in Gravitino's backend database in plain text and accessible to anyone with database access.

2. **Lack of Secret Rotation:** When catalog passwords are rotated, users must manually update the catalog, which exposes the new password again as plain text.

3. **Compliance Requirements:** Many industries (finance, healthcare, government) mandate external secret management for SOC2, HIPAA, PCI-DSS compliance.


### Industry Best Practices

Many organizations running applications in production already use an external secret management solution, such as:
- AWS Secrets Manager
- HashiCorp Vault
- Azure Key Vault
- Google Secret Manager
- Kubernetes Secrets with external secret operators

Gravitino should integrate seamlessly with these existing secret management infrastructures and provide an extensible framework to support any other vault providers.

## Current State

### Understanding CredentialProvider vs SecretProvider

It's important to clarify the relationship between two similar-sounding concepts:

| Component                          | Purpose                                                                              | Layer                | Changes Needed            |
|------------------------------------|--------------------------------------------------------------------------------------|----------------------|---------------------------|
| **CredentialProvider** (existing)  | Vends credentials to clients (Iceberg, Spark, etc.) for accessing cloud storage     | Application layer    | None - works transparently |
| **SecretProvider** (proposed)      | Fetches secrets from external systems (AWS Secrets Manager, Vault)                  | Infrastructure layer | New abstraction           |

**Key Point:** `SecretProvider` is a **prerequisite layer** that runs **before** `CredentialProvider`. It resolves secret references (`${secret:...}`) to actual values, then passes those resolved values to `CredentialProvider` implementations.

**Example Flow[Postgresql catalog]:**
```
User creates PostgreSQL catalog via UI with:
  jdbc-password = ${secret:prod-postgres-catalog/password}
              ↓
Gravitino stores the reference "${secret:prod-postgres-catalog/password}" in backend DB
              ↓
When catalog needs to connect to PostgreSQL:
  SecretProvider: Calls AWS Secrets Manager API → returns "mySecureP@ssw0rd"
              ↓
  Catalog/CredentialProvider: Receives resolved password "mySecureP@ssw0rd" and connects to PostgreSQL
              ↓ (no code changes needed!)
Client: Uses the PostgreSQL catalog to query tables
```

They work together at different layers, and existing `CredentialProvider` implementations require **zero code changes**.

### Catalog Credential Flow Today

```
┌─────────────────────────────────────────────────────────────┐
│  User Creates Catalog via UI/API                             │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ Catalog: "prod-postgres"                             │ │
│  │ Provider: "jdbc-postgresql"                          │ │
│  │ Properties:                                          │ │
│  │   jdbc-url = jdbc:postgresql://db.example.com/prod   │ │
│  │   jdbc-user = app_user                               │ │
│  │   jdbc-password = PlainTextPassword123               │ │
│  └───────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  Gravitino Backend Database                                  │
│  - Stores catalog properties including plain text password  │
│  - Password visible in database and logs                     │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  Catalog.initialize(properties)                              │
│  - Reads password from properties Map                        │
│  - Connects to external database with plain text password   │
└─────────────────────────────────────────────────────────────┘
```

## Proposed Solution

### High-Level Architecture

Introduce a **Secret Provider** abstraction layer that resolves secret references :

```
┌──────────────────────────────────────────────────────────────────┐
│  Gravitino Server Configuration (gravitino.conf)                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ # Global secret provider configuration                     │  │
│  │ secret-provider = aws-secrets-manager                      │  │
│  │ secret-provider.region = us-east-1                         │  │
│  │ secret-provider.secret-path-prefix = gravitino/prod/       │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│  SecretProviderFactory                                            │
│  - Creates appropriate SecretProvider based on configuration      │
└──────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│  SecretProvider Interface                                         │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ + resolveSecret(secretReference): String                   │  │
│  │ + resolveSecrets(secretReferences): Map<String, String>    │  │
│  │ + refreshSecrets(): void                                   │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
┌──────────────┐  ┌──────────────────┐  ┌─────────────────┐
│ AWS Secrets  │  │  HashiCorp       │  │  Plain Text     │
│ Manager      │  │  Vault           │  │  (Backward      │
│ Provider     │  │  Provider        │  │  Compatibility) │
└──────────────┘  └──────────────────┘  └─────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│  SecretCache                                                     │
│  - TTL-based caching                                             │
│  - Thread-safe access                                            │
└──────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│  CredentialProviders                                             │
│  - Receives resolved catalog credentials transparently           │
│  - No code changes required                                      │
└──────────────────────────────────────────────────────────────────┘
```

### Hybrid Approach: Global vs Explicit Provider (Catalog Properties)

The design supports **two complementary syntax patterns for catalog properties**:

1. **Global Provider (Recommended for single-tenant):**
   - Configure one secret provider in `gravitino.conf`
   - Use simple syntax in catalog properties: `${secret:path/to/secret}`
   - Simpler to manage, easier to migrate providers

2. **Explicit Provider (Multi-tenant scenarios):**
   - Configure multiple secret providers in `gravitino.conf`
   - Use provider-specific syntax in catalog properties: `${secret:provider-type:path/to/secret}`
   - Each catalog can use different secret management systems
   - Enables team-specific or compliance-driven provider selection

**Example (Catalog Property Values):**
```properties
# Catalog property using global provider
jdbc-password = ${secret:postgres/password}

# Catalog property with explicit provider override
jdbc-password = ${secret:vault:postgres/password}
```

This hybrid approach provides **simplicity by default** while enabling **flexibility when needed**.

## Goals (V1 Scope: Catalog Properties Only)

**Primary Focus:** Secret resolution for **catalog create/update/read operations only**.

1. **Provide pluggable external secret management** through a well-defined abstraction
2. **Implement AWS Secrets Manager integration** as the first reference implementation
3. **Support secret references in catalog properties** (`jdbc-password`, `jdbc-user`, Kerberos keytabs, etc.)
4. **Maintain backward compatibility** with existing plain-text catalog properties
5. **Implement API redaction rules** to prevent secret exposure in responses
6. **Minimize performance overhead** through intelligent caching

## Non-Goals (Deferred to Future Work)

1.  **Server configuration secret interpolation** (e.g., `gravitino.conf`, `gravitino-iceberg-rest-server.conf`) — **V2 feature**
2.  Encrypting Gravitino's internal metadata store (separate concern) 

## Design Details

### Core Components

#### 1. SecretReference Value Object

```java
package org.apache.gravitino.secret;

/**
 * Immutable value object representing a parsed secret reference.
 * 
 * <p>Supports formats:
 * - ${secret:path/to/secret}                    -> global provider, full secret
 * - ${secret:path/to/secret#key}                -> global provider, JSON key extraction
 * - ${secret:provider-type:path/to/secret}      -> explicit provider
 * - ${secret:provider-type:path/to/secret#key}  -> explicit provider with key
 * 
 * <p>This class validates the reference format at construction time and provides
 * structured access to the components, ensuring type safety and early error detection.
 * 
 * <p>After catalog creation, the reference is enriched with backend-specific metadata
 * (ARN, version-id, etc.) for version pinning and audit trail.
 */
public final class SecretReference {
  
  private static final Pattern SECRET_PATTERN = 
      Pattern.compile("\\$\\{secret:([^}]+)\\}");
  
  private static final Set<String> KNOWN_PROVIDERS = ImmutableSet.of(
      "aws-sm", "vault", "azure-kv", "gcp-sm", "k8s");
  
  private final String providerType;  // null means use default provider
  private final String secretPath;    // Path to the secret in the secret manager
  private final String jsonKey;       // null if not extracting from JSON
  private final String rawReference;  // Original reference for caching
  
  // Backend-specific metadata (ARN, version-id, timestamps, etc.)
  private final Map<String, String> metadata;
  
  private SecretReference(
      String providerType, 
      String secretPath, 
      String jsonKey,
      String rawReference,
      Map<String, String> metadata) {
    this.providerType = providerType;
    this.secretPath = Preconditions.checkNotNull(secretPath, "secretPath cannot be null");
    this.jsonKey = jsonKey;
    this.rawReference = rawReference;
    this.metadata = metadata != null ? ImmutableMap.copyOf(metadata) : ImmutableMap.of();
  }
  
  /**
   * Parses a secret reference string (without ${secret:...} wrapper).
   * Creates a base reference without metadata - call enrichReference() to add metadata.
   *
   * @param reference The reference string (e.g., "aws-sm:db/password#key")
   * @return Parsed SecretReference without metadata
   * @throws IllegalArgumentException if reference format is invalid
   */
  public static SecretReference parse(String reference) {
    Preconditions.checkNotNull(reference, "reference cannot be null");
    
    // Check if explicit provider is specified: provider-type:path
    String[] providerParts = reference.split(":", 2);
    String providerType = null;
    String pathAndKey;
    
    if (providerParts.length == 2 && isKnownProvider(providerParts[0])) {
      // Explicit provider syntax: aws-sm:path/to/secret
      providerType = providerParts[0];
      pathAndKey = providerParts[1];
    } else {
      // Global provider syntax: path/to/secret
      pathAndKey = reference;
    }
    
    // Check for JSON key extraction: path#key
    String[] pathParts = pathAndKey.split("#", 2);
    String secretPath = pathParts[0];
    String jsonKey = pathParts.length == 2 ? pathParts[1] : null;
    
    // Validate
    if (secretPath.isEmpty()) {
      throw new IllegalArgumentException(
          "Secret path cannot be empty in reference: " + reference);
    }
    
    return new SecretReference(providerType, secretPath, jsonKey, reference, null);
  }
  
  /**
   * Creates an enriched reference with metadata from the secret backend.
   * 
   * @param base Base reference from parse()
   * @param metadata Backend metadata (ARN, version-id, timestamps)
   * @return New SecretReference with metadata
   */
  public static SecretReference withMetadata(
      SecretReference base,
      Map<String, String> metadata) {
    return new SecretReference(
        base.providerType,
        base.secretPath,
        base.jsonKey,
        base.rawReference,
        metadata
    );
  }
  
  /**
   * Checks if a string contains a secret reference pattern.
   *
   * @param value The value to check
   * @return true if value contains ${secret:...} pattern
   */
  public static boolean isSecretReference(String value) {
    return value != null && SECRET_PATTERN.matcher(value).find();
  }
  
  /**
   * Extracts the reference content from ${secret:...} wrapper.
   *
   * @param value The wrapped reference (e.g., "${secret:db#password}")
   * @return The inner content (e.g., "db#password")
   * @throws IllegalArgumentException if not a valid secret reference
   */
  public static String extractReference(String value) {
    Matcher matcher = SECRET_PATTERN.matcher(value);
    if (matcher.find()) {
      return matcher.group(1);
    }
    throw new IllegalArgumentException("Not a secret reference: " + value);
  }
  
  /**
   * Parses a full secret reference including ${secret:...} wrapper.
   *
   * @param wrappedReference The full reference (e.g., "${secret:db#password}")
   * @return Parsed SecretReference
   */
  public static SecretReference parseWrapped(String wrappedReference) {
    String inner = extractReference(wrappedReference);
    return parse(inner);
  }
  
  private static boolean isKnownProvider(String name) {
    return KNOWN_PROVIDERS.contains(name);
  }
  
  /**
   * Returns the explicit provider type, or null if using default provider.
   */
  @Nullable
  public String getProviderType() {
    return providerType;
  }
  
  /**
   * Returns the secret path (without JSON key suffix).
   */
  public String getSecretPath() {
    return secretPath;
  }
  
  /**
   * Returns the JSON key to extract, or null if returning entire secret.
   */
  @Nullable
  public String getJsonKey() {
    return jsonKey;
  }
  
  /**
   * Returns backend-specific metadata (ARN, version-id, timestamps, etc.).
   * Empty map if not yet enriched.
   */
  @Nonnull
  public Map<String, String> getMetadata() {
    return metadata;
  }
  
  /**
   * Returns true if this reference uses an explicit provider.
   */
  public boolean hasExplicitProvider() {
    return providerType != null;
  }
  
  /**
   * Returns true if this reference requires JSON key extraction.
   */
  public boolean hasJsonKey() {
    return jsonKey != null;
  }
  
  /**
   * Returns the raw reference string for caching purposes.
   */
  public String getRawReference() {
    return rawReference;
  }
  
  /**
   * Builds a URN for structured storage and logging.
   * Format: urn:gravitino-secret:<provider>:<path>
   */
  public String toUrn() {
    StringBuilder urn = new StringBuilder("urn:gravitino-secret:");
    if (providerType != null) {
      urn.append(providerType);
    } else {
      urn.append("default");
    }
    urn.append(":").append(secretPath);
    return urn.toString();
  }
  
  /**
   * Serializes to JSON for storage in catalog entity.
   */
  public Map<String, Object> toJson() {
    Map<String, Object> json = new LinkedHashMap<>();
    json.put("providerType", providerType != null ? providerType : "default");
    json.put("secretPath", secretPath);
    if (jsonKey != null) {
      json.put("jsonKey", jsonKey);
    }
    if (!metadata.isEmpty()) {
      json.put("metadata", metadata);
    }
    return json;
  }
  
  /**
   * Deserializes from JSON storage.
   */
  public static SecretReference fromJson(Map<String, Object> json) {
    String providerType = (String) json.get("providerType");
    String secretPath = (String) json.get("secretPath");
    String jsonKey = (String) json.get("jsonKey");
    
    @SuppressWarnings("unchecked")
    Map<String, String> metadata = (Map<String, String>) json.get("metadata");
    
    return new SecretReference(
        "default".equals(providerType) ? null : providerType,
        secretPath,
        jsonKey,
        null, // rawReference not stored
        metadata
    );
  }
  
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SecretReference)) return false;
    SecretReference that = (SecretReference) o;
    return Objects.equals(rawReference, that.rawReference);
  }
  
  @Override
  public int hashCode() {
    return Objects.hash(rawReference);
  }
  
  @Override
  public String toString() {
    return "SecretReference{" + rawReference + "}";
  }
}
```

#### 2. SecretValue Value Object

```java
package org.apache.gravitino.secret;

/**
 * Immutable value object representing a resolved secret value.
 * 
 * <p>This class wraps the actual secret string and provides:
 * - Protection against accidental logging (toString() returns redacted form)
 * - Metadata about when the secret was loaded and its expiry
 * - Support for redacted representations in API responses
 * 
 * <p>Security Note: Always use getValue() explicitly when you need the actual
 * secret value. Never log or persist SecretValue.getValue() output.
 */
public final class SecretValue {
  
  private final String value;
  private final Instant loadedAt;
  private final Instant expiresAt;
  private final String version;  // Optional: version/ARN from secret manager
  
  private SecretValue(
      String value,
      Instant loadedAt,
      Instant expiresAt,
      String version) {
    this.value = Preconditions.checkNotNull(value, "secret value cannot be null");
    Preconditions.checkArgument(!value.isEmpty(), "secret value cannot be empty");
    this.loadedAt = loadedAt;
    this.expiresAt = expiresAt;
    this.version = version;
  }
  
  /**
   * Creates a SecretValue with metadata.
   */
  public static SecretValue of(
      String value,
      Instant loadedAt,
      Instant expiresAt,
      String version) {
    return new SecretValue(value, loadedAt, expiresAt, version);
  }
  
  /**
   * Creates a SecretValue with default metadata (current time, no version).
   */
  public static SecretValue of(String value, Duration ttl) {
    Instant now = Instant.now();
    return new SecretValue(value, now, now.plus(ttl), null);
  }
  
  /**
   * Creates a SecretValue with no expiry metadata.
   */
  public static SecretValue of(String value) {
    return new SecretValue(value, Instant.now(), null, null);
  }
  
  /**
   * Returns the actual secret value.
   * 
   * <p>SECURITY WARNING: Never log or persist this value.
   * Only use this when you need to pass the secret to external systems
   * (databases, cloud services, etc.).
   */
  public String getValue() {
    return value;
  }
  
  /**
   * Returns when this secret was loaded from the secret manager.
   */
  public Instant getLoadedAt() {
    return loadedAt;
  }
  
  /**
   * Returns when this secret expires from cache, or null if no expiry.
   */
  @Nullable
  public Instant getExpiresAt() {
    return expiresAt;
  }
  
  /**
   * Returns the secret version/ARN from the secret manager, or null if not available.
   */
  @Nullable
  public String getVersion() {
    return version;
  }
  
  /**
   * Returns true if this secret has expired.
   */
  public boolean isExpired() {
    return expiresAt != null && Instant.now().isAfter(expiresAt);
  }
  
  /**
   * Returns a redacted string representation suitable for API responses.
   * Shows reference info but masks the actual value.
   */
  public String toRedactedString() {
    return "SecretValue{loadedAt=" + loadedAt + ", expiresAt=" + expiresAt + ", value=***}";
  }
  
  /**
   * Returns redacted representation to prevent accidental logging.
   * NEVER includes the actual secret value.
   */
  @Override
  public String toString() {
    return "SecretValue{***}";
  }
  
  /**
   * Equality based on the actual secret value only.
   * Metadata differences don't affect equality.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof SecretValue)) return false;
    SecretValue that = (SecretValue) o;
    return Objects.equals(value, that.value);
  }
  
  @Override
  public int hashCode() {
    return Objects.hash(value);
  }
}
```

#### 3. SecretProvider Interface

```java
package org.apache.gravitino.secret;

/**
 * An interface for resolving secret references to actual secret values.
 * 
 * <p>Implementations provide integration with external secret management
 * systems like AWS Secrets Manager, HashiCorp Vault, Azure Key Vault, etc.
 * 
 * <p>Secret providers should implement caching and refresh strategies to
 * minimize external API calls while ensuring secrets are reasonably fresh.
 */
public interface SecretProvider extends Closeable {
  
  /**
   * Initializes the secret provider with configuration properties.
   *
   * @param properties Configuration properties specific to the provider
   */
  void initialize(Map<String, String> properties);
  
  /**
   * Resolves a single secret reference to its actual value.
   *
   * @param secretReference The parsed secret reference
   * @return The resolved secret value, or null if not found
   * @throws SecretResolutionException if secret resolution fails
   */
  @Nullable
  SecretValue resolveSecret(SecretReference secretReference);
  
  /**
   * Resolves multiple secret references in batch for efficiency.
   *
   * @param secretReferences Collection of secret references to resolve
   * @return Map of secret references to their resolved values
   * @throws SecretResolutionException if batch resolution fails
   */
  Map<SecretReference, SecretValue> resolveSecrets(Collection<SecretReference> secretReferences);
  
  /**
   * Returns the provider type identifier.
   *
   * @return Provider type (e.g., "aws-secrets-manager", "vault")
   */
  String providerType();
  
  /**
   * Enriches a secret reference with backend-specific metadata.
   * 
   * <p>Called during catalog creation to:
   * 1. Validate the secret exists in the backend
   * 2. Fetch metadata (ARN, version-id, creation timestamp)
   * 3. Return enriched reference for storage
   * 
   * <p>The enriched reference enables:
   * - Version pinning (prevents race conditions during secret rotation)
   * - Audit trail (when/how the secret was referenced)
   * - Performance (direct ARN lookup instead of name-based)
   * 
   * <p>Default implementation returns the reference as-is (no enrichment).
   * Providers should override this to add backend-specific metadata.
   * 
   * @param secretReference Base reference from user configuration
   * @return Enriched reference with metadata populated
   * @throws SecretResolutionException if secret doesn't exist or validation fails
   */
  default SecretReference enrichReference(SecretReference secretReference) {
    // Default: no enrichment
    return secretReference;
  }
  
  /**
   * Forces a refresh of cached secrets from the external system.
   * This is typically called on a schedule or when secrets are suspected stale.
   */
  void refreshSecrets();
}
```

#### 4. SecretReferenceResolver

Handles parsing and resolving secret references in configuration values:

```java
package org.apache.gravitino.secret;

/**
 * Resolves configuration properties that contain secret references.
 * 
 * <p>Supports hybrid syntax:
 * - Global provider: ${secret:reference-name} or ${secret:path/to/secret#key}
 * - Explicit provider: ${secret:provider-type:reference-name} or ${secret:provider-type:secret#key}
 * 
 * <p>Examples:
 * - ${secret:postgres-password} -> uses global provider
 * - ${secret:db-creds#password} -> uses global provider, extracts "password" key from JSON
 * - ${secret:aws-sm:prod-db/password} -> explicitly uses AWS Secrets Manager
 * - ${secret:vault:hive/keytab} -> explicitly uses HashiCorp Vault
 * - Plain text values are returned as-is for backward compatibility
 */
public class SecretReferenceResolver {
  
  private final SecretProvider defaultProvider;
  private final Map<String, SecretProvider> providerMap;
  
  public SecretReferenceResolver(
      SecretProvider defaultProvider,
      Map<String, SecretProvider> additionalProviders) {
    this.defaultProvider = defaultProvider;
    this.providerMap = additionalProviders;
  }
  
  /**
   * Resolves all secret references in a properties map.
   * 
   * <p>Secret references are resolved to their actual string values.
   * Plain text values are passed through unchanged for backward compatibility.
   *
   * @param properties Configuration properties that may contain secret references
   * @return New map with secret references resolved to actual values (as strings)
   */
  public Map<String, String> resolveSecrets(Map<String, String> properties) {
    Map<String, String> resolved = new HashMap<>();
    
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      
      if (SecretReference.isSecretReference(value)) {
        // Parse and resolve the secret reference
        String innerRef = SecretReference.extractReference(value);
        SecretReference secretRef = SecretReference.parse(innerRef);
        SecretValue secretValue = resolveSecretReference(secretRef);
        // Extract the actual string value for use in configuration
        resolved.put(key, secretValue.getValue());
      } else {
        // Not a secret reference - pass through as-is (backward compatibility)
        resolved.put(key, value);
      }
    }
    
    return resolved;
  }
  
  /**
   * Checks if a value contains a secret reference.
   */
  public boolean isSecretReference(String value) {
    return SecretReference.isSecretReference(value);
  }
  
  /**
   * Resolves a single secret reference to its value.
   * 
   * @param secretRef The parsed secret reference
   * @return The resolved SecretValue
   * @throws SecretResolutionException if resolution fails
   */
  private SecretValue resolveSecretReference(SecretReference secretRef) {
    // Determine which provider to use
    SecretProvider provider;
    if (secretRef.hasExplicitProvider()) {
      // Use explicitly specified provider
      provider = providerMap.get(secretRef.getProviderType());
      if (provider == null) {
        throw new SecretResolutionException(
            "Secret provider not configured: " + secretRef.getProviderType());
      }
    } else {
      // Use default provider
      provider = defaultProvider;
      if (provider == null) {
        throw new SecretResolutionException(
            "No default secret provider configured");
      }
    }
    
    // Resolve the secret
    SecretValue secretValue = provider.resolveSecret(secretRef);
    if (secretValue == null) {
      throw new SecretResolutionException(
          "Secret not found: " + secretRef.getRawReference());
    }
    
    return secretValue;
  }
}
```

#### 5. SecretCache

```java
package org.apache.gravitino.secret;

/**
 * Thread-safe cache for resolved secrets with TTL-based expiration.
 * 
 * <p>Provides automatic background refresh before expiration to ensure
 * continuous availability of secrets without cache misses.
 * 
 * <p>Cache key is the SecretReference object for efficient lookups.
 * Values are cached as SecretValue objects with metadata.
 */
public class SecretCache {
  
  private static final Logger LOG = LoggerFactory.getLogger(SecretCache.class);
  
  private final LoadingCache<SecretReference, SecretValue> cache;
  private final SecretProvider provider;
  private final ScheduledExecutorService refreshExecutor;
  private final Duration ttl;
  private final Duration refreshBeforeExpiry;
  
  public SecretCache(
      SecretProvider provider, 
      Duration ttl,
      Duration refreshBeforeExpiry,
      int maxSize) {
    this.provider = provider;
    this.ttl = ttl;
    this.refreshBeforeExpiry = refreshBeforeExpiry;
    this.refreshExecutor = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder()
            .setNameFormat("secret-cache-refresh-%d")
            .setDaemon(true)
            .build());
    
    this.cache = Caffeine.newBuilder()
        .maximumSize(maxSize)
        .expireAfterWrite(ttl)
        .build(key -> loadSecret(key));
    
    // Schedule background refresh at 80% of TTL
    long refreshIntervalMs = refreshBeforeExpiry.toMillis();
    refreshExecutor.scheduleAtFixedRate(
        this::refreshExpiringSecrets,
        refreshIntervalMs,
        refreshIntervalMs,
        TimeUnit.MILLISECONDS);
  }
  
  /**
   * Gets a secret from cache, loading from provider if necessary.
   * 
   * @return SecretValue, or null if secret not found
   */
  @Nullable
  public SecretValue get(SecretReference secretReference) {
    try {
      return cache.get(secretReference);
    } catch (Exception e) {
      LOG.error("Failed to load secret: {}", secretReference, e);
      return null;
    }
  }
  
  /**
   * Gets multiple secrets in batch.
   * 
   * @return Map of references to their resolved values (excludes not-found secrets)
   */
  public Map<SecretReference, SecretValue> getAll(
      Collection<SecretReference> secretReferences) {
    try {
      return cache.getAll(secretReferences);
    } catch (Exception e) {
      LOG.error("Failed to load secrets in batch", e);
      return Collections.emptyMap();
    }
  }
  
  /**
   * Loads a secret from the provider (cache miss).
   * 
   * @return SecretValue with TTL-based expiry metadata
   */
  private SecretValue loadSecret(SecretReference secretRef) {
    SecretValue secretValue = provider.resolveSecret(secretRef);
    
    if (secretValue == null) {
      throw new SecretResolutionException(
          "Secret not found: " + secretRef.getRawReference());
    }
    
    // If provider didn't set expiry, use cache TTL
    if (secretValue.getExpiresAt() == null) {
      return SecretValue.of(
          secretValue.getValue(),
          secretValue.getLoadedAt(),
          Instant.now().plus(ttl),
          secretValue.getVersion());
    }
    
    return secretValue;
  }
  
  /**
   * Proactively refreshes secrets approaching expiration.
   * Runs in background to prevent cache misses.
   */
  private void refreshExpiringSecrets() {
    Instant now = Instant.now();
    Instant refreshThreshold = now.plus(refreshBeforeExpiry);
    
    cache.asMap().forEach((ref, secretValue) -> {
      if (secretValue.getExpiresAt() != null 
          && secretValue.getExpiresAt().isBefore(refreshThreshold)) {
        try {
          LOG.debug("Proactively refreshing secret: {}", ref);
          cache.refresh(ref);
        } catch (Exception e) {
          LOG.warn("Failed to refresh secret: {}", ref, e);
          // Don't fail - old value still usable until expiry
        }
      }
    });
  }
  
  /**
   * Invalidates all cached secrets, forcing reload on next access.
   */
  public void invalidateAll() {
    cache.invalidateAll();
  }
  
  /**
   * Invalidates a specific secret, forcing reload on next access.
   */
  public void invalidate(SecretReference secretRef) {
    cache.invalidate(secretRef);
  }
  
  /**
   * Returns cache statistics for monitoring.
   */
  public CacheStats getStats() {
    return cache.stats();
  }
  
  /**
   * Closes the cache and stops background refresh.
   */
  public void close() {
    refreshExecutor.shutdown();
    try {
      if (!refreshExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        refreshExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      refreshExecutor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }
}
```

#### 6. AWS Secrets Manager Provider (Reference Implementation)

```java
package org.apache.gravitino.secret.aws;

/**
 * SecretProvider implementation for AWS Secrets Manager.
 * 
 * <p>Configuration properties:
 * - secret-provider.region: AWS region (required)
 * - secret-provider.secret-path-prefix: Prefix for all secret names
 * - secret-provider.endpoint: Custom endpoint for testing/LocalStack
 * - secret-provider.cache-ttl-seconds: Cache TTL (default: 300)
 * - secret-provider.cache-max-size: Max cached secrets (default: 100)
 * 
 * <p>Authentication via standard AWS credential provider chain:
 * 1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
 * 2. System properties
 * 3. Web identity token (EKS IRSA)
 * 4. EC2 instance profile
 * 5. ECS task role
 */
public class AwsSecretsManagerProvider implements SecretProvider {
  
  private static final Logger LOG = LoggerFactory.getLogger(AwsSecretsManagerProvider.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  
  private SecretsManagerClient secretsManagerClient;
  private SecretCache cache;
  private String secretPathPrefix;
  private Duration cacheTtl;
  
  @Override
  public void initialize(Map<String, String> properties) {
    String region = properties.get("secret-provider.region");
    String endpoint = properties.get("secret-provider.endpoint");
    this.secretPathPrefix = properties.getOrDefault(
        "secret-provider.secret-path-prefix", "");
    
    SecretsManagerClientBuilder builder = SecretsManagerClient.builder()
        .region(Region.of(region));
    
    if (endpoint != null) {
      builder.endpointOverride(URI.create(endpoint));
    }
    
    this.secretsManagerClient = builder.build();
    
    int ttlSeconds = Integer.parseInt(
        properties.getOrDefault("secret-provider.cache-ttl-seconds", "300"));
    int maxSize = Integer.parseInt(
        properties.getOrDefault("secret-provider.cache-max-size", "100"));
    
    this.cacheTtl = Duration.ofSeconds(ttlSeconds);
    this.cache = new SecretCache(
        this, 
        cacheTtl,
        Duration.ofSeconds(ttlSeconds / 5), // Refresh at 80% of TTL
        maxSize);
  }
  
  @Override
  public SecretReference enrichReference(SecretReference secretRef) {
    String secretName = buildSecretName(secretRef);
    
    try {
      // Validate secret exists and get metadata from AWS
      DescribeSecretRequest request = DescribeSecretRequest.builder()
          .secretId(secretName)
          .build();
      
      DescribeSecretResponse response = awsClient.describeSecret(request);
      
      // Get current version ID (AWSCURRENT stage)
      String versionId = response.versionIdsToStages().entrySet().stream()
          .filter(e -> e.getValue().contains("AWSCURRENT"))
          .map(Map.Entry::getKey)
          .findFirst()
          .orElse(null);
      
      // Build metadata map
      Map<String, String> metadata = new HashMap<>();
      metadata.put("arn", response.arn());
      if (versionId != null) {
        metadata.put("version-id", versionId);
      }
      metadata.put("created-at", response.createdDate().toString());
      metadata.put("last-accessed-at", Instant.now().toString());
      
      LOG.info("Enriched secret reference: {} -> ARN: {}, version: {}", 
          secretName, response.arn(), versionId);
      
      // Return enriched reference
      return SecretReference.withMetadata(secretRef, metadata);
      
    } catch (ResourceNotFoundException e) {
      throw new SecretResolutionException(
          "Secret not found in AWS Secrets Manager: " + secretName + 
          ". Please create the secret before creating the catalog.", e);
    } catch (SecretsManagerException e) {
      throw new SecretResolutionException(
          "Failed to validate secret in AWS Secrets Manager: " + secretName, e);
    }
  }
  
  @Override
  public SecretValue resolveSecret(SecretReference secretRef) {
    return cache.get(secretRef);
  }
  
  @Override
  public Map<SecretReference, SecretValue> resolveSecrets(
      Collection<SecretReference> secretReferences) {
    return cache.getAll(secretReferences);
  }
  
  /**
   * Loads a secret from AWS Secrets Manager (called by cache on miss).
   * Package-private for testing.
   * 
   * @param secretRef The secret reference to load
   * @return The resolved SecretValue with metadata
   */
  SecretValue loadSecret(SecretReference secretRef) {
    String secretName = buildSecretName(secretRef);
    
    // Use version from metadata if available (version pinning)
    String versionId = secretRef.getMetadata().get("version-id");
    
    try {
      GetSecretValueRequest.Builder requestBuilder = GetSecretValueRequest.builder()
          .secretId(secretName);
      
      if (versionId != null) {
        requestBuilder.versionId(versionId); // Pin to specific version
        LOG.debug("Resolving secret with pinned version: {} (version: {})", 
            secretName, versionId);
      } else {
        LOG.debug("Resolving secret without version pinning: {}", secretName);
      }
      
      GetSecretValueResponse response = 
          secretsManagerClient.getSecretValue(requestBuilder.build());
      
      // Handle both string and JSON secrets
      if (response.secretString() != null) {
        String secretValue = parseSecretValue(secretRef, response.secretString());
        
        // Build SecretValue with metadata from AWS response
        Instant now = Instant.now();
        return SecretValue.of(
            secretValue,
            now,
            now.plus(cacheTtl),
            response.versionId() // AWS version ID
        );
      } else {
        throw new SecretResolutionException(
            "Binary secrets not supported: " + secretName);
      }
    } catch (ResourceNotFoundException e) {
      LOG.warn("Secret not found in AWS Secrets Manager: {}", secretName);
      return null;
    } catch (SecretsManagerException e) {
      throw new SecretResolutionException(
          "Failed to retrieve secret: " + secretName, e);
    }
  }
  
  /**
   * Parses secret value, supporting both plain text and JSON with key selection.
   * 
   * Examples:
   * - SecretReference("s3-credentials", null) -> returns entire secret string
   * - SecretReference("s3-credentials", "access-key-id") -> parses JSON and returns specific key
   */
  private String parseSecretValue(SecretReference secretRef, String secretString) {
    if (secretRef.hasJsonKey()) {
      String key = secretRef.getJsonKey();
      try {
        JsonNode jsonNode = OBJECT_MAPPER.readTree(secretString);
        JsonNode valueNode = jsonNode.get(key);
        if (valueNode == null) {
          throw new SecretResolutionException(
              "JSON key not found in secret: " + key);
        }
        return valueNode.asText();
      } catch (JsonProcessingException e) {
        throw new SecretResolutionException(
            "Failed to parse JSON secret: " + secretRef.getRawReference(), e);
      }
    }
    return secretString;
  }
  
  /**
   * Builds the full secret name for AWS Secrets Manager API.
   * Applies the configured prefix and uses only the secret path (not the JSON key).
   */
  private String buildSecretName(SecretReference secretRef) {
    String path = secretRef.getSecretPath();
    return secretPathPrefix.isEmpty() ? path : secretPathPrefix + path;
  }
  
  @Override
  public String providerType() {
    return "aws-secrets-manager";
  }
  
  @Override
  public void refreshSecrets() {
    cache.invalidateAll();
  }
  
  @Override
  public void close() throws IOException {
    if (secretsManagerClient != null) {
      secretsManagerClient.close();
    }
    if (cache != null) {
      cache.close();
    }
  }
}
```

#### 7. SecretProviderFactory

```java
package org.apache.gravitino.secret;

/**
 * Factory for creating SecretProvider instances.
 * 
 * <p>Supports hybrid approach:
 * - Creates default/global provider from "secret-provider" config
 * - Creates additional providers from "secret-provider.{type}.enabled" configs
 * 
 * Uses ServiceLoader pattern for discovery, similar to CredentialProvider.
 */
public class SecretProviderFactory {
  
  private static final Map<String, Class<? extends SecretProvider>> PROVIDERS = 
      new ConcurrentHashMap<>();
  
  static {
    // Discover providers via ServiceLoader
    ServiceLoader<SecretProvider> loader = 
        ServiceLoader.load(SecretProvider.class);
    for (SecretProvider provider : loader) {
      PROVIDERS.put(provider.providerType(), provider.getClass());
    }
  }
  
  /**
   * Creates default secret provider and additional providers for multi-tenant.
   *
   * @param properties Configuration properties
   * @return SecretProviderManager containing default and additional providers
   */
  public static SecretProviderManager createSecretProviders(
      Map<String, String> properties) {
    
    // Create default provider
    String defaultProviderType = properties.get("secret-provider");
    SecretProvider defaultProvider = createProvider(defaultProviderType, properties);
    
    // Create additional providers for explicit syntax
    Map<String, SecretProvider> additionalProviders = new HashMap<>();
    for (String providerType : PROVIDERS.keySet()) {
      String enabledKey = "secret-provider." + providerType + ".enabled";
      if ("true".equals(properties.get(enabledKey))) {
        SecretProvider provider = createProvider(providerType, 
            filterPropertiesForProvider(properties, providerType));
        additionalProviders.put(providerType, provider);
      }
    }
    
    return new SecretProviderManager(defaultProvider, additionalProviders);
  }
  
  /**
   * Creates a single secret provider instance.
   */
  private static SecretProvider createProvider(
      String providerType,
      Map<String, String> properties) {
    
    // Backward compatibility: no provider = plain text
    if (providerType == null || "none".equals(providerType)) {
      return new PlainTextSecretProvider();
    }
    
    Class<? extends SecretProvider> providerClass = PROVIDERS.get(providerType);
    if (providerClass == null) {
      throw new IllegalArgumentException(
          "Unknown secret provider type: " + providerType);
    }
    
    try {
      SecretProvider provider = providerClass.getDeclaredConstructor()
          .newInstance();
      provider.initialize(properties);
      return provider;
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to create secret provider: " + providerType, e);
    }
  }
  
  /**
   * Filters properties for a specific provider.
   * E.g., "secret-provider.vault.address" -> "address"
   */
  private static Map<String, String> filterPropertiesForProvider(
      Map<String, String> properties, 
      String providerType) {
    String prefix = "secret-provider." + providerType + ".";
    return properties.entrySet().stream()
        .filter(e -> e.getKey().startsWith(prefix))
        .collect(Collectors.toMap(
            e -> e.getKey().substring(prefix.length()),
            Map.Entry::getValue));
  }
}

/**
 * Manages multiple secret providers for hybrid approach.
 */
class SecretProviderManager {
  private final SecretProvider defaultProvider;
  private final Map<String, SecretProvider> additionalProviders;
  
  SecretProviderManager(
      SecretProvider defaultProvider,
      Map<String, SecretProvider> additionalProviders) {
    this.defaultProvider = defaultProvider;
    this.additionalProviders = additionalProviders;
  }
  
  public SecretProvider getDefaultProvider() {
    return defaultProvider;
  }
  
  public SecretProvider getProvider(String providerType) {
    return additionalProviders.get(providerType);
  }
  
  public Map<String, SecretProvider> getAllProviders() {
    return Collections.unmodifiableMap(additionalProviders);
  }
}
```

### Secret Reference Enrichment

When a catalog is created with secret references, Gravitino **validates and enriches** the references with backend-specific metadata before storing them in the metadata store.

#### Why Enrichment?

1. **Early Validation**: Ensures the secret exists before catalog creation succeeds
2. **Version Pinning**: Captures current version to prevent race conditions during secret rotation
3. **Audit Trail**: Records when/how the secret was referenced
4. **Performance**: Stores physical identifiers (ARN) for faster resolution

#### Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│ User API Request                                            │
│ POST /api/metalakes/my_metalake/catalogs                    │
│ {                                                           │
│   "properties": {                                           │
│     "jdbc-password": "${secret:aws-sm:path#password}"       │
│   }                                                         │
│ }                                                           │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ Step 1: Parse Reference                                     │
│ SecretReference.parse("aws-sm:path#password")               │
│ → providerType: "aws-sm"                                    │
│ → secretPath: "path"                                        │
│ → jsonKey: "password"                                       │
│ → metadata: {} (empty)                                      │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ Step 2: Enrich with Backend Metadata                        │
│ awsProvider.enrichReference(baseRef)                        │
│ → Calls AWS DescribeSecret API                             │
│ → Validates secret exists                                   │
│ → Fetches ARN: arn:aws:...                                  │
│ → Fetches version-id: v1-abc123                             │
│ → Returns enriched reference                                │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ Step 3: Store in PostgreSQL                                 │
│ secret_references: {                                        │
│   "jdbc-password": {                                        │
│     "providerType": "aws-sm",                               │
│     "secretPath": "path",                                   │
│     "jsonKey": "password",                                  │
│     "metadata": {                                           │
│       "arn": "arn:aws:...",                                 │
│       "version-id": "v1-abc123",                            │
│       "created-at": "2026-04-16T10:30:00Z"                  │
│     }                                                       │
│   }                                                         │
│ }                                                           │
└─────────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────────┐
│ Step 4: Resolve During Catalog Initialization              │
│ awsProvider.resolveSecret(enrichedRef)                      │
│ → Uses version-id for pinning                               │
│ → GetSecretValue(versionId: "v1-abc123")                    │
│ → Always gets same version (consistent!)                    │
└─────────────────────────────────────────────────────────────┘
```

#### Code Example

**Catalog Creation**:
```java
public class CatalogManager {
  
  @Inject
  private SecretProviderManager secretProviderManager;
  
  public Catalog createCatalog(CreateCatalogRequest request) {
    Map<String, String> properties = request.getProperties();
    Map<String, SecretReference> secretReferences = new HashMap<>();
    
    // Process properties to extract and enrich secret references
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      
      if (SecretReference.isSecretReference(value)) {
        // Parse: ${secret:aws-sm:gravitino/prod/postgres#password}
        String innerRef = SecretReference.extractReference(value);
        SecretReference baseRef = SecretReference.parse(innerRef);
        
        // Enrich with backend metadata (calls AWS)
        SecretProvider provider = secretProviderManager.getProvider(
            baseRef.getProviderType());
        SecretReference enrichedRef = provider.enrichReference(baseRef);
        
        // Store enriched reference
        secretReferences.put(key, enrichedRef);
        
        // Remove from public properties
        properties.remove(key);
      }
    }
    
    // Create catalog entity with separated properties
    CatalogEntity entity = CatalogEntity.builder()
        .withId(idGenerator.nextId())
        .withName(request.getName())
        .withProperties(properties)              // Public properties
        .withSecretReferences(secretReferences)  // Secret references with metadata
        .build();
    
    // Persist to metadata store
    store.put(entity);
    
    return entity;
  }
}
```

**Secret Resolution**:
```java
public class JdbcCatalog implements Catalog {
  
  @Inject
  private SecretProvider secretProvider;
  
  @Override
  public void initialize(Map<String, String> properties, 
                        Map<String, SecretReference> secretReferences) {
    String jdbcUrl = properties.get("jdbc-url");
    String jdbcUser = properties.get("jdbc-user");
    
    // Resolve secret using enriched reference
    SecretReference passwordRef = secretReferences.get("jdbc-password");
    SecretValue secretValue = secretProvider.resolveSecret(passwordRef);
    
    // Extract actual password
    String jdbcPassword = secretValue.getValue();
    
    // Create datasource
    dataSource = DataSourceBuilder.create()
        .url(jdbcUrl)
        .username(jdbcUser)
        .password(jdbcPassword)
        .build();
  }
}
```

#### Benefits

✅ **Fail Fast**: Catalog creation fails immediately if secret doesn't exist  
✅ **Version Consistency**: Always uses same secret version (no race conditions)  
✅ **Audit Trail**: Know exactly which secret version was used when  
✅ **Performance**: ARN lookup faster than name-based lookup in AWS  
✅ **Security**: Track secret access patterns for compliance  

#### Error Handling

**Secret doesn't exist**:
```json
POST /api/metalakes/my_metalake/catalogs
→ 400 Bad Request

{
  "code": 1003,
  "type": "SecretResolutionException",
  "message": "Secret not found in AWS Secrets Manager: gravitino/prod/postgres. Please create the secret before creating the catalog.",
  "stack": [...]
}
```

**JSON key not found**:
```json
POST /api/metalakes/my_metalake/catalogs
→ 400 Bad Request

{
  "code": 1003,
  "type": "SecretResolutionException",
  "message": "JSON key 'password' not found in secret gravitino/prod/postgres. Available keys: [username, host, port]"
}
```

**Insufficient permissions**:
```json
POST /api/metalakes/my_metalake/catalogs
→ 403 Forbidden

{
  "code": 1004,
  "type": "SecretResolutionException",
  "message": "Access denied to secret gravitino/prod/postgres. Ensure the IAM role has secretsmanager:GetSecretValue and secretsmanager:DescribeSecret permissions."
}
```

### Catalog Entity Storage Structure

Gravitino stores catalog entities with separate storage for public properties and secret references.

#### Database Schema

**PostgreSQL Table**:
```sql
CREATE TABLE catalog_entity (
  catalog_id BIGINT PRIMARY KEY,
  metalake_id BIGINT NOT NULL,
  name VARCHAR(255) NOT NULL,
  type VARCHAR(50) NOT NULL,
  provider VARCHAR(100) NOT NULL,
  comment TEXT,
  
  -- Public properties (non-sensitive)
  properties JSONB NOT NULL DEFAULT '{}'::JSONB,
  
  -- Secret references with metadata (sensitive property keys)
  secret_references JSONB NOT NULL DEFAULT '{}'::JSONB,
  
  audit_info JSONB,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
  
  UNIQUE(metalake_id, name)
);

-- Index for querying by secret provider type
CREATE INDEX idx_catalog_secret_provider 
ON catalog_entity USING GIN (secret_references);
```

#### Example Storage

**Catalog Entity Row**:
```sql
SELECT catalog_id, name, properties, secret_references 
FROM catalog_entity 
WHERE catalog_id = 123;
```

**Result**:

| Column | Value |
|--------|-------|
| **catalog_id** | `123` |
| **name** | `prod-postgres` |
| **type** | `RELATIONAL` |
| **provider** | `jdbc-postgresql` |
| **properties** | `{"jdbc-url": "jdbc:postgresql://db:5432/prod", "jdbc-user": "app_user"}` |
| **secret_references** | See below ⬇️ |

**`secret_references` JSONB Column**:
```json
{
  "jdbc-password": {
    "providerType": "aws-sm",
    "secretPath": "gravitino/prod/postgres-catalog",
    "jsonKey": "password",
    "metadata": {
      "arn": "arn:aws:secretsmanager:us-east-1:123456789012:secret:gravitino/prod/postgres-catalog",
      "version-id": "v1-abc123-def456",
      "created-at": "2026-04-15T08:30:00Z",
      "last-accessed-at": "2026-04-16T10:30:00Z"
    }
  }
}
```

#### Key Design Principles

1. ✅ **Actual secret values NEVER stored in database**
2. ✅ **Only references with metadata stored**
3. ✅ **Public properties separate from secret references**
4. ✅ **JSONB type enables efficient querying and indexing**

#### Query Examples

**Find all catalogs using AWS Secrets Manager**:
```sql
SELECT catalog_id, name
FROM catalog_entity
WHERE secret_references @> '{"jdbc-password": {"providerType": "aws-sm"}}'::jsonb;
```

**Find catalogs using a specific secret ARN**:
```sql
SELECT catalog_id, name,
       secret_references->'jdbc-password'->'metadata'->>'arn' as secret_arn
FROM catalog_entity
WHERE secret_references->'jdbc-password'->'metadata'->>'arn' 
      = 'arn:aws:secretsmanager:us-east-1:123456789012:secret:gravitino/prod/postgres-catalog';
```

**Find catalogs with secrets older than 90 days**:
```sql
SELECT catalog_id, name,
       secret_references->'jdbc-password'->'metadata'->>'created-at' as secret_created
FROM catalog_entity
WHERE (secret_references->'jdbc-password'->'metadata'->>'created-at')::timestamp 
      < NOW() - INTERVAL '90 days';
```

**Audit: When was a secret last accessed**:
```sql
SELECT catalog_id, name,
       secret_references->'jdbc-password'->'metadata'->>'last-accessed-at' as last_access
FROM catalog_entity
WHERE secret_references ? 'jdbc-password'
ORDER BY (secret_references->'jdbc-password'->'metadata'->>'last-accessed-at')::timestamp DESC;
```

### Secret Reference Syntax

Support flexible secret reference formats with **hybrid approach** - global provider (simpler) or explicit provider (multi-tenant):

#### Global Provider Syntax (Recommended)

Uses the globally configured `secret-provider` from configuration:

1. **Simple reference:** `${secret:secret-name}`
   - Uses global provider
   - Resolves to entire secret value
   - Example: `${secret:postgres-password}` → `mySecurePassword`

2. **JSON key reference:** `${secret:secret-name#key}`
   - Uses global provider
   - Parses JSON secret and extracts specific key
   - Example: `${secret:db-creds#password}` → extracts `password` from JSON

3. **Path-based reference:** `${secret:path/to/secret}`
   - Uses global provider
   - Supports hierarchical secret organization
   - Example: `${secret:gravitino/prod/postgres-catalog/password}`

#### Explicit Provider Syntax (Multi-Tenant)

Allows overriding the global provider for specific secrets:

4. **Provider-specific reference:** `${secret:provider-type:secret-name}`
   - Explicitly specifies which provider to use
   - Example: `${secret:aws-sm:prod-postgres/password}` → uses AWS Secrets Manager
   - Example: `${secret:vault:hive/keytab}` → uses HashiCorp Vault

5. **Provider-specific with JSON key:** `${secret:provider-type:secret-name#key}`
   - Combines explicit provider with JSON key extraction
   - Example: `${secret:aws-sm:db-creds#password}`

#### Backward Compatibility

6. **Plain text:** `actual-value`
   - If no `${secret:...}` syntax, treated as literal value
   - Example: `MyPlainPassword` → `MyPlainPassword`

#### Provider Type Identifiers

- `aws-sm` - AWS Secrets Manager
- `vault` - HashiCorp Vault
- `azure-kv` - Azure Key Vault
- `gcp-sm` - Google Secret Manager
- `k8s` - Kubernetes Secrets

### Service Discovery

Use Java ServiceLoader for provider discovery:

```
META-INF/services/org.apache.gravitino.secret.SecretProvider
```

Content:
```
org.apache.gravitino.secret.aws.AwsSecretsManagerProvider
org.apache.gravitino.secret.vault.VaultSecretProvider (future)
org.apache.gravitino.secret.azure.AzureKeyVaultProvider (future)
```

## Configuration Examples

### Example 1: Global Provider - PostgreSQL Catalog

**Step 1: Store secrets in AWS Secrets Manager**
```bash
# Create secret for PostgreSQL catalog credentials
aws secretsmanager create-secret \
    --name gravitino/prod/postgres-catalog \
    --secret-string '{
      "jdbc-user": "app_user",
      "jdbc-password": "SuperSecurePassword123"
    }' \
    --region us-east-1
```

**Step 2: Configure Gravitino with global secret provider**
```properties
# gravitino.conf

# Enable AWS Secrets Manager as global provider
secret-provider = aws-secrets-manager
secret-provider.region = us-east-1
secret-provider.secret-path-prefix = gravitino/prod/
secret-provider.cache-ttl-seconds = 300
```

**Step 3: Create catalog via UI/API with secret references**
```json
// POST /api/metalakes/{metalake}/catalogs
{
  "name": "prod-postgres",
  "type": "RELATIONAL",
  "provider": "jdbc-postgresql",
  "properties": {
    "jdbc-url": "jdbc:postgresql://db.example.com:5432/production",
    "jdbc-user": "${secret:postgres-catalog#jdbc-user}",
    "jdbc-password": "${secret:postgres-catalog#jdbc-password}",
    "jdbc-driver": "org.postgresql.Driver"
  }
}
```

**Result:** Gravitino stores the references `${secret:...}` in its backend DB and resolves them at runtime using AWS Secrets Manager.

### Example 2: Explicit Provider Syntax - Multi-Tenant Scenario

For organizations where different teams use different secret management systems:

**Step 1: Configure multiple secret providers**
```properties
# gravitino.conf

# Global provider (default)
secret-provider = aws-secrets-manager
secret-provider.region = us-east-1
secret-provider.secret-path-prefix = gravitino/prod/

# Additional providers for multi-tenant
secret-provider.vault.enabled = true
secret-provider.vault.address = https://vault.example.com
secret-provider.vault.namespace = gravitino

secret-provider.k8s.enabled = true
secret-provider.k8s.namespace = gravitino-secrets
```

**Step 2: Create catalogs with explicit provider references**

**Team A - Uses AWS Secrets Manager (global default):**
```json
// PostgreSQL catalog using global provider
{
  "name": "team-a-postgres",
  "type": "RELATIONAL",
  "provider": "jdbc-postgresql",
  "properties": {
    "jdbc-url": "jdbc:postgresql://team-a-db.example.com/prod",
    "jdbc-user": "team_a_user",
    "jdbc-password": "${secret:team-a/postgres#password}"
  }
}
```

**Team B - Uses HashiCorp Vault (explicit):**
```json
// MySQL catalog explicitly using Vault
{
  "name": "team-b-mysql",
  "type": "RELATIONAL",
  "provider": "jdbc-mysql",
  "properties": {
    "jdbc-url": "jdbc:mysql://team-b-db.example.com:3306/prod",
    "jdbc-user": "team_b_user",
    "jdbc-password": "${secret:vault:team-b/mysql/password}"
  }
}
```

**Team C - Uses Kubernetes Secrets (explicit):**
```json
// Hive catalog using K8s secrets
{
  "name": "team-c-hive",
  "type": "RELATIONAL", 
  "provider": "hive",
  "properties": {
    "metastore.uris": "thrift://hive-metastore:9083",
    "authentication.type": "KERBEROS",
    "authentication.kerberos.principal": "${secret:k8s:team-c-hive-principal}",
    "authentication.kerberos.keytab-uri": "${secret:k8s:team-c-hive-keytab}"
  }
}
```

**Result:** Each team can use their preferred secret management system while sharing the same Gravitino instance.

### Example 3: Backward Compatible (No Secret Provider)

```properties
# No secret provider configured - plain text works as before
credential-providers = s3-secret-key
s3-access-key-id = AKIAIOSFODNN7EXAMPLE
s3-secret-access-key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

### Example 4: LocalStack for Testing

```properties
# gravitino.conf - Use LocalStack for local development/testing
secret-provider = aws-secrets-manager
secret-provider.region = us-east-1
secret-provider.endpoint = http://localhost:4566
secret-provider.secret-path-prefix = gravitino/dev/
```

Then create catalogs with secret references:
```json
{
  "name": "dev-postgres",
  "type": "RELATIONAL",
  "provider": "jdbc-postgresql",
  "properties": {
    "jdbc-url": "jdbc:postgresql://localhost:5432/dev",
    "jdbc-user": "dev_user",
    "jdbc-password": "${secret:dev-postgres#password}"
  }
}
```

## Testing Strategy

### Unit Tests

1. **SecretReferenceResolver Tests**
   - Parse various secret reference formats
   - Handle malformed references
   - Backward compatibility with plain text

2. **SecretCache Tests**
   - TTL expiration
   - Background refresh
   - Thread safety under concurrent load
   - Cache eviction policies

3. **AwsSecretsManagerProvider Tests**
   - Mock AWS SDK calls
   - Test error handling (secret not found, throttling, network errors)
   - JSON parsing for key extraction
   - Prefix handling

### Integration Tests

1. **LocalStack Integration**
   ```java
   @Test
   @Tag("gravitino-docker-test")
   public void testAwsSecretsManagerWithLocalStack() {
     // Start LocalStack container with Secrets Manager
     // Create secrets via AWS SDK
     // Configure Gravitino to use LocalStack endpoint
     // Verify secret resolution
   }
   ```

2. **End-to-End Credential Vending**
   - Create catalog with secret references
   - Verify credentials generated correctly
   - Test credential refresh on rotation

3. **Performance Tests**
   - Measure secret resolution latency
   - Test cache hit rates
   - Concurrent secret access

### Security Tests

1. **Secret Masking in Logs**
   - Verify secrets never logged
   - Check error messages don't leak secrets

2. **Permission Tests**
   - Verify IAM policy enforcement
   - Test access denied scenarios

## Alternatives Considered

### Alternative 1: Environment Variable Substitution

**Approach:** Support `${env:VAR_NAME}` syntax in configuration.

**Pros:**
- Simple implementation
- Wide platform support
- No external dependencies

**Cons:**
- Environment variables visible in process listings (`ps aux`)
- No secret rotation without process restart
- No centralized audit logging

### Alternative 2: App-managed key encryption

**Approach:** Use static key supplied through application configs and encrypt sensitive data in gravitino backend store itself.

**Pros:**
- Full control over implementation
- Easy to implement 
**Cons:**
- Less secure and not possible to rotate the encryption keys if compromised 

## Future Work

### Phase 2: Server Configuration Secret Interpolation (V2)

**Extend secret resolution beyond catalog properties to server configuration:**

#### Motivation: Why External Secret Management for Server Config?

**Security Standards Perspective:**

Modern security best practices and compliance standards (SOC2, HIPAA, PCI-DSS, NIST 800-53) recommend **never storing secrets in configuration files**. Two common approaches exist:

| Approach | Security Profile | Use Case |
|----------|------------------|----------|
| **External Secret Manager (Recommended)** |  High - Secrets never in config files<br> Runtime rotation without redeployment<br> Centralized audit logging<br> Encrypted at rest in secret manager<br>⚠️ Requires external dependency | Production deployments, enterprises with compliance requirements |
| **Deploy-time Injection (Alternative)** | ⚠️ Medium - Secrets in Kubernetes ConfigMaps/Secrets<br> Base64 encoded, not encrypted by default<br> Visible via kubectl<br> Requires redeployment for rotation<br> No runtime dependencies | Development, air-gapped environments |

**Recommendation:** Use external secret management (runtime resolution) for production deployments. This aligns with how AWS EKS, Google GKE, and Azure AKS secure production workloads.

#### Scope

1. **Gravitino Server Config (`gravitino.conf`)**
   - Support secret references in main server configuration
   - Use cases: database credentials for metadata store, internal service credentials
   
2. **Iceberg REST Server Config**
   - Support secret references for credential provider configuration
   ```properties
   # gravitino-iceberg-rest-server.conf
   gravitino.iceberg-rest.credential-providers = s3-token
   gravitino.iceberg-rest.s3-access-key-id = ${secret:s3-creds#access-key}
   gravitino.iceberg-rest.s3-secret-access-key = ${secret:s3-creds#secret-key}
   gravitino.iceberg-rest.s3-role-arn = ${secret:s3-creds#role-arn}
   ```

3. **Auxiliary Service Configurations**
   - Support for any future auxiliary services
   - Consistent secret reference syntax across all config files

#### Solving the Bootstrap Problem

**Challenge:** How does Gravitino authenticate to the secret manager if the authentication credentials are also secrets?

**Solution: Two-Phase Initialization with Clear Separation**

```properties
# gravitino.conf

# ================================================================
# PHASE 1: Bootstrap Configuration (MUST be plain text or K8s env)
# Secret provider configuration itself CANNOT use secret references
# ================================================================

# Secret provider type and connection details
secret-provider = aws-secrets-manager
secret-provider.region = us-east-1
secret-provider.cache-ttl-seconds = 300

# Authentication: Use IAM Roles (IRSA/Workload Identity) - NO credentials needed
# OR fall back to AWS SDK default credential chain (env vars from K8s Secrets)

# ================================================================
# PHASE 2: Application Configuration (Secret references allowed)
# Resolved AFTER SecretProvider is initialized
# ================================================================

# Metadata store credentials
gravitino.entity.store.relational.jdbcUrl = jdbc:postgresql://db:5432/gravitino
gravitino.entity.store.relational.jdbcUser = ${secret:gravitino-metastore/username}
gravitino.entity.store.relational.jdbcPassword = ${secret:gravitino-metastore/password}

# Iceberg REST S3 credentials
gravitino.iceberg-rest.s3-access-key-id = ${secret:iceberg-s3/access-key}
gravitino.iceberg-rest.s3-secret-access-key = ${secret:iceberg-s3/secret-key}
gravitino.iceberg-rest.s3-role-arn = ${secret:iceberg-s3/role-arn}

# OAuth authentication
gravitino.authenticator.oauth.defaultSignKey = ${secret:gravitino-oauth/signing-key}
gravitino.authenticator.oauth.clientId = ${secret:oauth-provider/client-id}

# Kerberos keytab (for Hive/HDFS)
gravitino.authenticator.kerberos.keytab-uri = ${secret:kerberos/keytab-base64}
```

**Bootstrap Authentication Options:**

**Option 1: Cloud IAM Roles (Recommended for Production)**

AWS EKS with IRSA:
```yaml
# Kubernetes ServiceAccount with IAM role annotation
apiVersion: v1
kind: ServiceAccount
metadata:
  name: gravitino
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/GravitinoSecretAccess
```

Google GKE with Workload Identity:
```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: gravitino
  annotations:
    iam.gke.io/gcp-service-account: gravitino@project-id.iam.gserviceaccount.com
```

Azure AKS with Managed Identity:
```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: gravitino
  annotations:
    azure.workload.identity/client-id: <client-id>
```

**Option 2: Environment Variables from Kubernetes Secrets (Fallback)**

```yaml
# Kubernetes Secret for secret manager authentication
apiVersion: v1
kind: Secret
metadata:
  name: gravitino-secret-provider-auth
type: Opaque
data:
  aws-access-key-id: <base64>
  aws-secret-access-key: <base64>

---
# Pod spec references the secret
spec:
  containers:
  - name: gravitino
    env:
    - name: AWS_ACCESS_KEY_ID
      valueFrom:
        secretKeyRef:
          name: gravitino-secret-provider-auth
          key: aws-access-key-id
    - name: AWS_SECRET_ACCESS_KEY
      valueFrom:
        secretKeyRef:
          name: gravitino-secret-provider-auth
          key: aws-secret-access-key
```

Gravitino uses AWS SDK default credential chain, which automatically picks up env vars.

#### Configuration Loading Order

```
1. Load gravitino.conf from disk
2. Parse secret-provider.* properties (Phase 1)
3. Initialize SecretProvider with Phase 1 config
4. Authenticate to secret manager using:
   - Cloud IAM role (IRSA/Workload Identity) OR
   - Environment variables from K8s Secrets OR
   - AWS SDK default credential chain
5. Parse remaining properties (Phase 2)
6. Resolve all ${secret:...} references using SecretProvider
7. Start Gravitino with fully resolved configuration
```

#### Security Benefits

Compared to deploy-time injection (Helm values.yaml → ConfigMaps):

| Security Aspect | External Secret Manager | Deploy-time Injection |
|-----------------|------------------------|----------------------|
| **Secrets in config files** |  Never |  Yes (in ConfigMaps) |
| **Secrets in git history** |  Never (only references) | ⚠️ Risk if values.yaml committed |
| **Secrets visible in K8s** |  No (only in secret manager) |  Yes via `kubectl get configmap -o yaml` |
| **Encryption at rest** |  AWS KMS / Vault transit | ⚠️ Only if K8s etcd encryption enabled |
| **Secret rotation** |  Without redeployment |  Requires pod restart |
| **Audit logging** |  Centralized (who/when/what) | ⚠️ Limited (K8s audit logs only) |
| **Compliance** |  SOC2/HIPAA/PCI-DSS compliant | ⚠️ May not meet requirements |
| **Defense in depth** |  Config file leak doesn't expose secrets |  Config/ConfigMap leak exposes secrets |

#### Example: Full Server Configuration with Secrets

```properties
# gravitino.conf - Production Configuration

# ================================================================
# PHASE 1: Secret Provider Bootstrap (Plain Text)
# ================================================================
secret-provider = aws-secrets-manager
secret-provider.region = us-east-1
secret-provider.cache-ttl-seconds = 300
secret-provider.cache-max-size = 200
secret-provider.secret-path-prefix = gravitino/prod/

# ================================================================
# PHASE 2: Application Configuration (Secret References)
# ================================================================

# Gravitino metadata store
gravitino.entity.store = relational
gravitino.entity.store.relational = JDBCBackend
gravitino.entity.store.relational.jdbcUrl = jdbc:postgresql://prod-db.example.com:5432/gravitino
gravitino.entity.store.relational.jdbcDriver = org.postgresql.Driver
gravitino.entity.store.relational.jdbcUser = ${secret:metastore-db#username}
gravitino.entity.store.relational.jdbcPassword = ${secret:metastore-db#password}

# Iceberg REST service
gravitino.auxService.names = iceberg-rest
gravitino.iceberg-rest.catalog-backend = jdbc
gravitino.iceberg-rest.uri = jdbc:postgresql://iceberg-metastore.example.com:5432/iceberg
gravitino.iceberg-rest.jdbc-user = ${secret:iceberg-metastore-db#username}
gravitino.iceberg-rest.jdbc-password = ${secret:iceberg-metastore-db#password}
gravitino.iceberg-rest.warehouse = s3://prod-datalake/warehouse/

# S3 credential vending (Gravitino's own S3 credentials)
gravitino.iceberg-rest.credential-providers = s3-token
gravitino.iceberg-rest.s3-access-key-id = ${secret:iceberg-s3-admin#access-key-id}
gravitino.iceberg-rest.s3-secret-access-key = ${secret:iceberg-s3-admin#secret-access-key}
gravitino.iceberg-rest.s3-role-arn = ${secret:iceberg-s3-admin#role-arn}
gravitino.iceberg-rest.s3-region = us-east-1

# OAuth authentication
gravitino.authenticators = oauth
gravitino.authenticator.oauth.serverUri = https://auth.example.com
gravitino.authenticator.oauth.defaultSignKey = ${secret:oauth-config#signing-key}
gravitino.authenticator.oauth.serviceAudience = ${secret:oauth-config#audience}
```

**AWS Secrets Manager Secret Structure:**

```json
// Secret: gravitino/prod/metastore-db
{
  "username": "gravitino_app",
  "password": "SuperSecurePassword123!"
}

// Secret: gravitino/prod/iceberg-metastore-db
{
  "username": "iceberg_admin",
  "password": "AnotherSecurePassword456!"
}

// Secret: gravitino/prod/iceberg-s3-admin
{
  "access-key-id": "AKIAIOSFODNN7EXAMPLE",
  "secret-access-key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
  "role-arn": "arn:aws:iam::123456789012:role/IcebergDataAccess"
}

// Secret: gravitino/prod/oauth-config
{
  "signing-key": "base64-encoded-jwt-signing-key",
  "audience": "gravitino-production"
}
```

#### Design Considerations

1. **Clear Configuration Boundaries:**
   - Secret provider configuration: MUST be plain text or K8s env vars
   - Everything else: CAN use secret references
   - Document which properties can use `${secret:...}` syntax

2. **Error Handling:**
   - If SecretProvider initialization fails → Gravitino MUST fail fast with clear error
   - If secret resolution fails during Phase 2 → Log which secret failed, fail startup
   - Provide validation CLI tool: `gravitino-server.sh validate-config`

3. **Configuration Validation:**
   - Detect circular dependencies (secret provider config referencing secrets)
   - Warn if plain-text secrets detected in config (suggest migration)
   - Validate secret reference syntax at startup

4. **Backward Compatibility:**
   - If `secret-provider` not configured → All values treated as plain text
   - Existing plain-text configs continue to work
   - Migration path: Add secret provider → Replace values with references → Test

5. **Testing:**
   - Unit tests with LocalStack for AWS Secrets Manager
   - Integration tests for bootstrap scenarios
   - Security tests: Verify secrets never logged

#### Alternative: Deploy-time Injection

For users who cannot use external secret managers (air-gapped, development):

```yaml
# values.yaml - Helm chart
icebergRest:
  s3:
    # Reference Kubernetes Secret
    accessKeyId: "{{ .Values.secrets.s3AccessKey }}"
    secretAccessKey: "{{ .Values.secrets.s3SecretKey }}"

# Kubernetes Secret (created separately)
---
apiVersion: v1
kind: Secret
metadata:
  name: gravitino-secrets
type: Opaque
data:
  s3AccessKey: <base64>
  s3SecretKey: <base64>
```

**Trade-offs:**
-  Simpler, no runtime dependencies
-  Secrets in ConfigMaps (visible via kubectl)
-  Rotation requires redeployment



### Phase 3: Additional Secret Providers

1. **HashiCorp Vault Provider**
   - Support KV v1 and v2
   - AppRole and Kubernetes authentication
   - Dynamic secret generation
   
2. **Azure Key Vault Provider**
   - Managed Identity authentication
   - Secret versioning

3. **Google Secret Manager Provider**
   - Workload Identity authentication
   - IAM integration

4. **Kubernetes Secrets Provider**
   - Direct integration with k8s secret API
   - Support for External Secrets Operator



### Phase 4: Advanced Features

1. **Secret Versioning**
   - Support pinning to specific secret versions
   - Gradual rollout of rotated secrets

2. **Multi-Region Secret Replication**
   - Automatic failover to replica regions
   - Cross-region secret synchronization

3. **Metrics and Monitoring**
   ```
   gravitino.secret.resolution.time.seconds
   gravitino.secret.cache.hit.ratio
   gravitino.secret.refresh.failures
   ```

## Backward Compatibility

**100% backward compatible:**

1. **No secret provider configured:** Plain text configuration works as before
2. **Existing catalogs:** No changes required unless migrating to secret provider
3. **API compatibility:** No changes to public APIs
4. **Configuration format:** Existing configs remain valid

**Migration is opt-in** at operator discretion.

## Open Questions (V1 Scope)

1. **Secret rotation detection strategy (V1)**
   - **Option A:** Rely purely on cache TTL for eventual consistency
     - Simpler implementation, no external dependencies
     - Secret changes visible within cache TTL window (e.g., 5 minutes)
   - **Option B:** Proactive polling with change detection
     - Check secret version/timestamp on scheduled interval
     - Only refresh if changed, reducing API calls

2. **Catalog initialization failure behavior (V1)**
   - If secret provider fails during catalog initialization, should Gravitino:
     - **Fail fast** (recommended): Catalog initialization fails, user gets clear error
     - **Defer resolution**: Initialize catalog but fail on first actual connection attempt

3. **Performance target for V1 (catalog operations only)**
   - Acceptable latency for secret resolution during catalog initialization:
     - Cached: <50ms (in-memory lookup)
     - Cache miss: <250ms (external API call to AWS Secrets Manager)
   - Should we fail catalog creation if secret resolution exceeds timeout?
   - **Recommendation:** 5-second timeout for catalog initialization secret resolution

4. **API response redaction rules (V1)**
   - When reading catalog properties via API, how should secret references be returned?
     - **Option A:** Return the reference as-is: `"jdbc-password": "${secret:db#password}"`
     - **Option B:** Redact completely: `"jdbc-password": "***"`
     - **Option C:** Show masked reference: `"jdbc-password": "${secret:db#***}"`
   - **Recommendation:** Option A (return reference as-is) for transparency and debuggability

5. **Backward compatibility guarantee**
   - Should we guarantee that catalogs created with plain-text passwords continue to work indefinitely?
   - Or set a deprecation timeline for plain-text catalog credentials?
   - **Recommendation:** Keep backward compatibility in V1, add deprecation warning in V2, require migration in V3 (2+ years)

## Summary

## V1 Scope: Catalog Properties Only

**This design is intentionally scoped to catalog properties for the initial implementation.**

### V1 Scope (Catalog Properties Only)
 **SecretProvider abstraction** with AWS Secrets Manager implementation  
 **Catalog property secret resolution** during create/update/read operations  
 **Caching layer** with configurable TTL for performance  
 **API redaction rules** to prevent secret exposure  
 **Backward compatibility** with plain-text catalog credentials  
 **Hybrid syntax** supporting both global and explicit provider references  
 **Comprehensive testing** with LocalStack integration  

### V2 and Beyond
 **Server configuration secret interpolation** (`gravitino.conf`, REST server configs)  
 **Additional providers** (K8s Secrets, Vault, Azure Key Vault, Google Secret Manager)  


**Key Takeaway:** V1 validates the abstraction with a focused scope (catalog properties only), enabling faster review and iteration. Server config support comes in V2 once the design is proven.

---

## References

1. [AWS Secrets Manager Documentation](https://docs.aws.amazon.com/secretsmanager/)
2. [Gravitino Credential Vending Documentation](docs/security/credential-vending.md)

