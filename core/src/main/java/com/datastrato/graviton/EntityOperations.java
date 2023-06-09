package com.datastrato.graviton;

public interface EntityOperations<E extends Entity & HasIdentifier, CREATE extends EntityCreate,
    CHANGE extends EntityChange> {

  /**
   * List the entities in the system.
   *
   * @return an array of entities
   */
  E[] listEntities();

  /**
   * Load an entity by {@link NameIdentifier} from the system.
   *
   * @param ident the identifier of the entity
   * @return the entity
   * @throws NoSuchEntityException if the entity does not exist
   */
  E loadEntity(NameIdentifier ident) throws NoSuchEntityException;

  /**
   * Check if an entity exists using an {@link NameIdentifier} from the system.
   *
   * @param ident the identifier of the entity
   * @return true if the entity exists, false otherwise
   */
  default boolean entityExists(NameIdentifier ident) {
    try {
      loadEntity(ident);
      return true;
    } catch (NoSuchEntityException e) {
      return false;
    }
  }

  /**
   * Create an entity in the system.
   *
   * @param ident the identifier of the entity
   * @param create the entity creation metadata
   * @return the created entity metadata
   * @throws EntityAlreadyExistsException if the entity already exists
   */
  E createEntity(NameIdentifier ident, CREATE create) throws EntityAlreadyExistsException;

  /**
   * Apply the {@link EntityChange} to alter an entity in the system.
   *
   * Implementation may reject the change. If any change is rejected, no changes should be
   * applied to the entity.
   *
   * @param ident the identifier of the entity
   * @param change the entity change to apply to the entity
   * @return the altered entity metadata
   * @throws NoSuchEntityException if the entity does not exist
   * @throws IllegalArgumentException if the change is rejected by the implementation
   */
  E alterEntity(NameIdentifier ident, CHANGE change)
      throws NoSuchEntityException, IllegalArgumentException;

  /**
   * Drop an entity from the system.
   *
   * @param ident the identifier of the entity
   * @return true if the entity was dropped, false if the entity did not exist
   */
  boolean dropEntity(NameIdentifier ident);
}
