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
- [ ] API is intuitive and follows existing patterns
- [ ] Backward compatible or breaking change is documented
- [ ] Proper use of interfaces vs. abstract classes
- [ ] Builder pattern for complex object construction

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

### 9. Code Organization

- [ ] Classes have single responsibility
- [ ] Methods are appropriately sized (< 100 lines typically)
- [ ] Proper separation of concerns (API/implementation/tests)
- [ ] Package structure is logical
- [ ] No circular dependencies between modules
- [ ] Utility classes are truly reusable
- [ ] Constants are properly defined and named

### 10. Documentation

- [ ] User-facing documentation updated in `docs/`
- [ ] Configuration examples provided
- [ ] Migration guides for breaking changes
- [ ] Code comments explain "why" not "what"
- [ ] Complex algorithms have explanatory comments
- [ ] Links to relevant issues/discussions

Remember: The goal is to maintain code quality while being welcoming and constructive to contributors. Balance rigor with pragmatism, and always assume good intent.
