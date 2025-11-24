<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# GitHub Copilot Code Review Instructions for Apache Gravitino

This document provides comprehensive guidelines for GitHub Copilot and other AI assistants when reviewing code contributions to the Apache Gravitino project.

## Core Review Principles

1. **Apache Foundation Standards**: Ensure all contributions follow Apache Software Foundation guidelines
2. **Backward Compatibility**: Breaking changes must be explicitly justified and documented
3. **Test Coverage**: All code changes must include appropriate tests - no exceptions
4. **Code Quality**: Prioritize maintainability, readability, and performance
5. **Documentation**: Public APIs and complex logic must be well-documented

## Code Review Checklist

### 1. License and Legal Compliance

- [ ] All new files have the Apache License 2.0 header
- [ ] No GPL or other incompatible licenses in dependencies
- [ ] Third-party code is properly attributed
- [ ] LICENSE and NOTICE files updated if dependencies added
- [ ] No copyrighted code without proper permissions

### 2. Code Quality

#### Java Code
- [ ] Follows Google Java Style Guide (enforced by Spotless)
- [ ] No wildcard imports
- [ ] Proper use of `@Nullable` annotations
- [ ] Exception handling is appropriate (don't catch generic `Exception`)
- [ ] Resources are properly closed (use try-with-resources)
- [ ] No `System.out.println()` - use proper logging
- [ ] Logging levels are appropriate (DEBUG, INFO, WARN, ERROR)
- [ ] No TODO or FIXME comments without issue references
- [ ] Thread safety is considered and documented where relevant
- [ ] No deprecated APIs unless necessary (document why)

#### Scala Code
- [ ] Follows Scala style conventions
- [ ] Proper use of immutable collections
- [ ] Pattern matching over if-else where appropriate
- [ ] Avoid `null` - use `Option` instead

#### Python Code
- [ ] Follows PEP 8 style guidelines
- [ ] Type hints for function parameters and return values
- [ ] Proper docstrings for public methods
- [ ] Use context managers for resource management

### 3. Testing Requirements

#### Unit Tests
- [ ] All new methods/functions have unit tests
- [ ] Edge cases and error conditions are tested
- [ ] Tests are independent and can run in any order
- [ ] Test names clearly describe what is being tested
- [ ] Assertions have meaningful failure messages
- [ ] No hard-coded timeouts (or well-justified)
- [ ] Mock external dependencies appropriately

#### Integration Tests
- [ ] End-to-end scenarios are covered
- [ ] Docker-dependent tests tagged with `@Tag("gravitino-docker-test")`
- [ ] Tests clean up resources (temp files, containers, etc.)
- [ ] Tests work in both embedded and deploy modes
- [ ] Connection pooling and resource limits considered

#### Test Anti-Patterns to Flag
- [ ] Tests that depend on execution order
- [ ] Tests with `Thread.sleep()` without justification
- [ ] Tests that ignore exceptions
- [ ] Tests without assertions
- [ ] Flaky tests (consider marking as such)

### 4. API Design

#### Public APIs
- [ ] Complete Javadoc with `@param`, `@return`, `@throws`
- [ ] Include code examples in Javadoc for complex APIs
- [ ] API is intuitive and follows existing patterns
- [ ] Backward compatible or breaking change is documented
- [ ] Proper use of interfaces vs. abstract classes
- [ ] Builder pattern for complex object construction
- [ ] Use `Into<T>` or `AsRef<T>` patterns where appropriate (for Rust-like flexibility)

#### REST APIs
- [ ] Follows RESTful conventions
- [ ] Proper HTTP status codes
- [ ] Request/response DTOs are properly validated
- [ ] API versioning considered
- [ ] Error responses are consistent and informative
- [ ] Authentication/authorization properly enforced

### 5. Performance Considerations

- [ ] No unnecessary object allocations in hot paths
- [ ] Database queries are efficient (proper indexing considered)
- [ ] Proper use of connection pooling
- [ ] Batch operations where appropriate
- [ ] Caching strategy is sound
- [ ] Memory leaks prevented (especially with caches)
- [ ] Large datasets handled with streaming/pagination
- [ ] Potential performance regressions identified

### 6. Security Review

- [ ] No hardcoded credentials or secrets
- [ ] Input validation for all external data
- [ ] SQL injection prevention (use prepared statements)
- [ ] Path traversal vulnerabilities prevented
- [ ] Proper access control checks
- [ ] Sensitive data is not logged
- [ ] Cryptographic operations use strong algorithms
- [ ] SSRF (Server-Side Request Forgery) protection

### 7. Configuration and Dependencies

- [ ] Configuration keys follow naming conventions
- [ ] Default values are sensible
- [ ] Configuration is validated on startup
- [ ] New dependencies are justified and minimal
- [ ] Dependency versions are appropriate (not too old/bleeding edge)
- [ ] Transitive dependencies reviewed for conflicts
- [ ] Dependencies compatible with Apache License

### 8. Error Handling

- [ ] Exceptions are meaningful and actionable
- [ ] Error messages help users understand what went wrong
- [ ] Stack traces include relevant context
- [ ] Graceful degradation where possible
- [ ] Retry logic is implemented with exponential backoff
- [ ] Circuit breakers for external service calls
- [ ] Proper cleanup in error paths

### 9. Concurrency and Thread Safety

- [ ] Shared mutable state is properly synchronized
- [ ] No race conditions in concurrent code
- [ ] Deadlock potential is minimized
- [ ] Thread pools are properly sized and configured
- [ ] Use of concurrent collections where appropriate
- [ ] Volatile/AtomicInteger/etc. used correctly
- [ ] CompletableFuture/reactive patterns used properly

### 10. Database and Storage

- [ ] Transactions used appropriately
- [ ] Proper index usage for queries
- [ ] N+1 query problems avoided
- [ ] Database migrations are backward compatible
- [ ] Connection leaks prevented
- [ ] Proper handling of database-specific features
- [ ] Storage operations are idempotent where possible

### 11. Code Organization

- [ ] Classes have single responsibility
- [ ] Methods are appropriately sized (< 100 lines typically)
- [ ] Proper separation of concerns (API/implementation/tests)
- [ ] Package structure is logical
- [ ] No circular dependencies between modules
- [ ] Utility classes are truly reusable
- [ ] Constants are properly defined and named

### 12. Documentation

- [ ] User-facing documentation updated in `docs/`
- [ ] Configuration examples provided
- [ ] Migration guides for breaking changes
- [ ] Architecture decisions documented (ADR if significant)
- [ ] Code comments explain "why" not "what"
- [ ] Complex algorithms have explanatory comments
- [ ] Links to relevant issues/discussions

### 13. Gravitino-Specific Checks

#### Catalog Implementations
- [ ] Extends `BaseCatalog` appropriately
- [ ] Implements all required operations
- [ ] Error handling for catalog-specific failures
- [ ] Configuration properly validated
- [ ] Tests cover catalog operations end-to-end

#### Connectors
- [ ] Follows connector pattern for the target engine
- [ ] Integration with Gravitino metadata is correct
- [ ] Connector-specific configurations documented
- [ ] Installation instructions provided
- [ ] Compatibility matrix updated

#### Metadata Operations
- [ ] Metadata changes are atomic where required
- [ ] Version handling is correct
- [ ] Audit logging included for sensitive operations
- [ ] Proper isolation between tenants/catalogs
- [ ] Metadata consistency maintained

### 14. Build and CI

- [ ] Build passes with `./gradlew build`
- [ ] Code formatted with Spotless (`./gradlew spotlessApply`)
- [ ] No compiler warnings introduced
- [ ] Tests pass in both embedded and deploy modes
- [ ] Docker tests pass (if applicable)
- [ ] Build time is reasonable (flag if significantly slower)

## Common Issues to Flag

### Critical Issues (Must Fix)
- Missing license headers
- Security vulnerabilities
- Test failures or missing tests
- Breaking API changes without justification
- Memory leaks
- Thread safety issues
- SQL injection vulnerabilities

### High Priority Issues
- Poor error handling
- Missing documentation for public APIs
- Performance regressions
- Inconsistent naming conventions
- Improper resource management
- Unnecessary dependencies

### Medium Priority Issues
- Code duplication
- Overly complex methods
- Missing Javadoc
- Inconsistent logging
- Suboptimal algorithms
- Missing edge case tests

### Low Priority Issues (Suggestions)
- Minor style inconsistencies
- Opportunities for refactoring
- Additional test coverage suggestions
- Documentation improvements
- Performance optimization opportunities

## Review Comments Style

When providing feedback:

1. **Be Specific**: Point to exact lines and explain the issue
2. **Be Constructive**: Suggest improvements, not just criticisms
3. **Provide Context**: Explain why something matters
4. **Reference Standards**: Link to coding standards or examples
5. **Acknowledge Good Work**: Highlight well-written code
6. **Ask Questions**: When intent is unclear, ask rather than assume

### Example Comment Templates

**For Missing Tests:**
```
⚠️ Missing test coverage for this method. Please add unit tests covering:
- Normal case with valid input
- Edge case with empty/null input  
- Error case when [specific condition]

Example:
[provide sample test structure]
```

**For API Documentation:**
```
📝 Public API missing Javadoc. Please add documentation including:
- Description of what the method does
- @param descriptions
- @return description
- @throws for exceptions
- Usage example

See [similar API] for reference.
```

**For Performance Concerns:**
```
⚡ Potential performance issue: This operation happens in a loop and creates N database queries.
Consider using batch operations or caching.

Suggested approach:
[provide alternative implementation]
```

**For Security Issues:**
```
🔒 Security concern: User input is not validated before [operation].
This could lead to [specific vulnerability].

Required changes:
- Add input validation
- Sanitize/escape data
- Add test for malicious input
```

## Helpful Resources

- Main documentation: https://gravitino.apache.org/docs/latest/
- Contribution guide: CONTRIBUTING.md
- Architecture details: AGENTS.md
- Apache guidelines: https://www.apache.org/dev/
- Java style guide: https://google.github.io/styleguide/javaguide.html

## When in Doubt

If a change is complex or potentially controversial:
1. Ask the contributor to clarify the intent
2. Suggest discussion on the dev@ mailing list
3. Request additional reviewers
4. Link to relevant documentation or examples
5. Propose iterative improvements in follow-up PRs

Remember: The goal is to maintain code quality while being welcoming and constructive to contributors. Balance rigor with pragmatism, and always assume good intent.
