package mjson.hgdb;

import mjson.Json;

import org.hypergraphdb.HGHandle;

/**
 * <p>
 * This interface provides options related to entity management in a JSON
 * data store. Entities are distinguished from other JSON elements in that they
 * are objects that represent real world entities that change through time. Unlike
 * other JSON elements, entities are mutable and carry an identity throughout
 * their lifetime. Other JSON elements that are stored are immutable values
 * and therefore their identity is not important, even though like all atoms in the
 * database they have HyperGraphDB handles.
 * </p>
 *
 * <p>
 * The default implementation {@link EntityInterfaceImpl} recognizes as entities
 * JSON objects that have an <code>entity</code> property of type <code>string</code>.
 * </p>
 */
public interface EntityInterface
{
    /**
     * <p>Return true if the JSON <code>object</code> represents a mutable entity.</p>
     * 
     * @param element The JSON object to test. The caller guarantees that the parameter
     * is in fact a JSON object.
     */
    boolean isEntity(Json objectElement);
    
    /**
     * <p>Lookup entity by value. When a JSON element is an entity coming from 
     * the outside, it may not have a <code>hghandle</code> property to identity it.
     * Yet, it may be already stored in the data store and identifiable by some
     * other means (e.g. a "primary key"). An implementation can provide a lookup
     * function to find the entity and avoid inserting a duplicate record.</p>
     *
     * <p>
     * The data store will enforce integrity constraints so that if an attempt is
     * made to add an entity with an already existing primary key, an exception will
     * be thrown.
     * </p>
     *
     * @return The <code>HGHandle</code> of an existing entity in the data store or
     * <code>null</code> if the entity could not be found.
     */
    HGHandle lookupEntity(HyperNodeJson node, Json entity);

    /**
     * <p>Return the name of the JSON property entity where to store the handle of entities
     *  or <code>null</code> if the handle is not going to be stored in entities.</p>
     */
    String entityHandleProperty();       
    
    /**
     * <p>Return true if the data store should permit that mutable entities be nested 
     * inside what would otherwise be an immutable value. Return false otherwise.</p>
     * <p>
     * By default, the database will throw an exception if you try to add a top-level 
     * <code>Json</code> that is not an entity, and is therefore immutable, but it contains
     * a nested element (e.g. a property or an array element) that is itself an entity.
     * Return true from this method to change that behavior and by more permissive.
     * </p>
     */
    boolean allowEntitiesInImmutableValues();
}