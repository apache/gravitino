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

# Policy-on-Tag Design Q&A

This document captures the remaining design decisions for the policy-on-tag approach. Policies are
associated with tags, objects receive effective governance behavior through their attached tags, tags
are flat identifiers, and policies are immutable after creation.

## Q1. How Should Tag and Policy Visibility Be Controlled?

Introduce two read-only visibility privileges:

```text
VIEW_TAG
VIEW_POLICY
```

`VIEW_TAG` allows users to list and get tag metadata. It does not allow attaching a tag to an
object.

`VIEW_POLICY` allows users to list and get policy metadata and policy details. It does not allow
applying, associating, updating, or deleting a policy.

Existing mutation privileges keep their current semantics:

```text
APPLY_TAG
APPLY_POLICY
```

`APPLY_TAG` controls whether a user can attach a tag to an object. `APPLY_POLICY` controls whether
a user can associate a policy with a tag.

Recommended binding scopes:

| Privilege | Binding Scope | Meaning |
|-----------|---------------|---------|
| `VIEW_TAG` | `METALAKE`, `TAG` | View, list, and get tag metadata. |
| `VIEW_POLICY` | `METALAKE`, `POLICY` | View, list, and get policy metadata and details. |
| `APPLY_TAG` | `METALAKE`, `TAG` | Attach a tag to an object. |
| `APPLY_POLICY` | `METALAKE`, `POLICY` | Associate a policy with a tag. |

Policy enforcement must be independent from policy visibility. A user does not need `VIEW_POLICY`
for row filters or column masks to take effect. The trusted server-side enforcement path must always
resolve and enforce applicable policies, even when the end user cannot see policy details.

For object policy APIs, users without `VIEW_POLICY` may receive only a redacted governance summary,
such as policy type and `enforced=true`. Users with `VIEW_POLICY` may receive policy names, types,
definitions, and other policy details. Source tag information should only be returned when the user
also has `VIEW_TAG`.

## Q2. How Should Row Filter and Column Mask Policy Conflicts Be Handled?

There are several common approaches across comparable systems:

| Approach | Example | Trade-off |
|----------|---------|-----------|
| Restrict conflicts at configuration time | Snowflake allows only one directly assigned row access policy on a table or view, and evaluates row access policies before masking policies. | Simple and predictable, but stricter for administrators. |
| Combine row filters with OR semantics | BigQuery combines multiple row-level access policies with OR semantics. | Flexible, but can broaden access and is risky as a default for tag-driven governance. |
| Priority-based resolution | Some systems can choose a winning policy by priority. | Flexible, but effective access becomes harder to reason about and easier to misconfigure. |
| Fail closed on ambiguity | Databricks ABAC blocks access when multiple distinct row filters or column masks apply to the same target. | Safest default, but administrators must fix overlapping tags or policy associations. |

This design chooses **fail closed**.

For row filter and column mask policies, if multiple distinct effective policies of the same kind
apply to the same evaluation target, access should be denied. The system should not merge them,
union them, or choose one by priority in this design.

Multiple identical policies may be deduplicated and enforced once. Administrators must consolidate
tags or policy associations before access is allowed.

## Q3. Should Tag Values Be Overwritten or Appended?

Use **overwrite** semantics, not append semantics.

For a given object, each tag key can have at most one effective value. Assigning the same tag key
with a new value replaces the existing direct value.

If inheritance is supported, a direct value should override the inherited value. Removing the direct
value should reveal the inherited value again.

Do not allow multi-value append behavior such as:

```text
sensitivity = high, restricted
```

Use one value per tag key:

```text
sensitivity = restricted
```

If multiple dimensions are needed, use multiple tag keys:

```text
sensitivity = restricted
domain = finance
retention = 7_years
```

Most comparable systems use key-unique, replace-or-override semantics:

| Product | Behavior |
|---------|----------|
| Snowflake | Tags are key-value pairs. A child object can override an inherited tag value. |
| AWS Lake Formation LF-Tags | A resource can have only one value for a given LF-Tag key. Adding the same key with a different value updates the existing value. |
| Databricks Unity Catalog | Tag keys are unique within a securable object. Duplicate tag keys are not treated as appended values. |
| Google Cloud tags and labels | Keys are unique on a resource. A direct tag value can override an inherited value. |
| BigQuery policy tags | A column can have only one policy tag. |

## References

- [Databricks policy evaluation and conflict handling](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/policy-evaluation)
- [Snowflake tag inheritance](https://docs.snowflake.com/en/user-guide/object-tagging/inheritance)
- [Snowflake row access policies](https://docs.snowflake.com/en/user-guide/security-row-intro)
- [AWS Lake Formation LF-Tag considerations](https://docs.aws.amazon.com/lake-formation/latest/dg/lf-tag-considerations.html)
- [Google Cloud tags](https://docs.cloud.google.com/resource-manager/docs/tags/tags-creating-and-managing)
- [BigQuery row-level security](https://docs.cloud.google.com/bigquery/docs/managing-row-level-security)
- [BigQuery column-level access control](https://docs.cloud.google.com/bigquery/docs/column-level-security-intro)
