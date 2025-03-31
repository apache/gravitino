---
title: "Distributed tables"
slug: /manage-distributed-tables
keyword: Table distributed
license: This software is licensed under the Apache License version 2.
---

## Distributed tables

To create a distributed (bucketed) table, you will provide the following parameters.

- <tt>strategy</tt>: How Gravitino distributes table data across partitions.
  See [below](#distribution-strategy) for a list of valid strategies.
- <tt>number</tt>: Number of buckets you use to distribution the table.
- <tt>funcArgs</tt>: The arguments of the strategy, the argument must be an [expression](../../expression.md).

### Distribution strategies

<table>
<thead>
<tr>
  <th>Distribution strategy</th>
  <th>Description</th>
  <th>JSON</th>
  <th>Java</th>
</tr>
</thead>
<tbody>
<tr>
  <td><tt>hash</tt></td>
  <td>
    Distribute table using a Hash function.
    Gravitino distributes the table data into buckets based on the hash value of the specified key.
  </td>
  <td>`hash`</td>
  <td>`Strategy.HASH`</td>
</tr>
<tr>
  <td><tt>range</tt></td>
  <td>
    Distribute table using range.
    Gravitino distributes the table data into buckets based on a specified range or interval of values.
  </td>
  <td>`range`</td>
  <td>`Strategy.RANGE`</td>
</tr>
<tr>
  <td><tt>even</tt></td>
  <td>
    Distribution table evenly.
    Gravitino distributes table data, ensuring an equal distribution of data.
    Currently we use `even` to implementation Doris `random` distribution.
  </td>
  <td>`even`</td>
  <td>`Strategy.EVEN`</td>
</tr>
</tbody>
</table>

### Defining distribution strategies

<Tabs groupId='language' queryString>
<TabItem value="Json" label="Json">

```json
{
  "strategy": "hash",
  "number": 4,
  "funcArgs": [
    {
      "type": "field",
      "fieldName": ["score"]
    }
  ]
}
```

</TabItem>
<TabItem value="java" label="Java">

```java
Distributions.of(Strategy.HASH, 4, NamedReference.field("score"));
```

</TabItem>
</Tabs>

