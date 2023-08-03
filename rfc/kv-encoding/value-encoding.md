<!--
  Copyright 2023 Datastrato.
  This software is licensed under the Apache License version 2.
-->

# Value encoding in key-value Store
| Revision | Owner    |
| :------- |----------|
| v0.1     | Qi Yu    |

## Background

Currently, We use protobuf protocol to encode the value in key-value store. which is very efficient and easy to use. But it is not easy to debug and read.
However, It can't support the following schema evolution scenarios:

Support we have tree structure like this:

- Catalog1
    - Schema1
    - Schema2
    - Broker1
    - Broker2
    - Namespace1


When we want to list all entities in namespace `Catalog1`, we will get a list of byte array that encoded by protobuf. But we don't know which entity is a schema, which entity is a broker, which entity is a namespace.
So we need some extra information to help us to decode the byte array to distinguish the entity type.

## Solution

The following two solutions are proposed to solve the problem. Both is to add some extra information at the header of the byte array.

### 1. Add a type identifier at the header of the byte array
We will introduce a map that store the mapping between type name and type identifier. When we encode the value, we will add the type identifier at the header of the byte array. When we decode the value, we will read the type identifier from the header of the byte array and then use the type identifier to get the type name from the maps. Then we can use the type name to decode the byte array.

The mapping will as following:

```java
private static final BiMap<String, Integer> ENTITY_TO_PROTO_ID = HashBiMap.create();

static {
ENTITY_TO_PROTO_ID.put(BaseMetalake.class.getCanonicalName(), 0);
}
```

Thus we can use 0 to represent the class name ‘BaseMetalake’

The final byte array will be like this:

| Header<br/>(20 bytes) | Object content |  
|-----------------------|----------------|


And the `Header` will be:

| class type<br/>(4 bytes) | reserved<br/>(16 bytes) |  
|--------------------------|--------------------|




### 2. Add the specific type name (class name) at the header of the byte array

This solution is similar to the first solution. The only difference is that we will add the specific type name at the header of the byte array instead of a identifier. The final byte array will be like this:


| Header| Object content |  
|-------|----------------|


And the `Header` will be:

| length of class name<br/>(4 bytes) | class name value<br/>(n bytes) |  
|--------------------------|--------------------------------|



As the length of class name is variable, Lengths of header in solution 2 is longer than solution 1 and we do not reserve more space for other messages in future.

## Performance

### Solution 1
Advantage:
It is very easy to implement. We just need to add the type identifier at the header of the byte array.
It is very efficient. Only takes 4 bytes to store the type identifier.
It can handle name changes and package changes.

Disadvantage:
It introduces 2 nested mappings and is not very straightforward to read.

### Solution 2
Advantage:
Very straightforward to read.

Disadvantage:
It may take much more space than option 1. Because we need to store the full class name at the header of the byte array.
It can't handle name changes and package changes.


After careful discussion and considering all possible use cases, We think option 1 is more proper and efficient and we will choose this option to encode value in key-value store.


## Implementation

We will introduce a new interface EntitySerDeFacade which is a facade for detailed serialization and deserialization implementation. Till now we only support protobuf protocol to store entity as byte array,  So default implementation of EntitySerDeFacade will use protobuf  as underlying serialization implementation.

```java
public interface EntitySerDeFacade {

/**
* Set the underlying entity ser/de implementation. {@link EntitySerDe} will be used to serialize
* and deserialize
*
* @param serDe The detailed implementation of {@link EntitySerDe}
  */
  void setEntitySeDe(EntitySerDe serDe);

/**
* Serializes the entity to a byte array. Compare to {@link EntitySerDe#serialize(Entity)}, this
* method will add some extra message to the serialized byte array, such as the entity class name.
* Other information can also be added in the future.
*
* @param t the entity to serialize
* @return the serialized byte array of the entity
* @param <T> The type of entity
* @throws IOException if the serialization fails
  */
  <T extends Entity> byte[] serialize(T t) throws IOException;

/**
* Deserializes the entity from a byte array.
*
* @param bytes the byte array to deserialize
* @param <T> The type of entity
* @return the deserialized entity
* @throws IOException if the deserialization fails
  */
  default <T extends Entity> T deserialize(byte[] bytes) throws IOException {
  ClassLoader loader =
  Optional.ofNullable(Thread.currentThread().getContextClassLoader())
  .orElse(getClass().getClassLoader());
  return deserialize(bytes, loader);
  }

/**
* Deserializes the entity from a byte array.
*
* @param bytes the byte array to deserialize
* @param classLoader the class loader to use
* @param <T> The type of entity
* @return the deserialized entity
* @throws IOException if the deserialization fails
  */
  <T extends Entity> T deserialize(byte[] bytes, ClassLoader classLoader) throws IOException;
  }
```


When we want to deserialize the byte array, we will read the type identifier from the header of the byte array and then use the type identifier to get the type name from the maps. Then we can use the type name to deserialize the byte array.


