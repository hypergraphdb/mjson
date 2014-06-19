package mjson.hgdb;

import mjson.Json;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGSearchResult;

/**
 * <p>
 * The default implementation {@link EntityInterfaceImpl} recognizes as entities
 * JSON objects that have an <code>entity</code> property of type <code>string</code>.
 * </p>
 */
public class EntityInterfaceImpl implements EntityInterface
{
    private boolean attachHandleFlag = true;
    private boolean allowEntitiesInValuesFlag = false;
    private String primaryKey = null;

    /**
     * <p>Return true if the object has a property named <Code>entity</code> whose value is a string.</p>
     */
    public boolean isEntity(Json object)
    {
        if (!object.isObject())
            return false;
        Json p = object.at("entity");
        return p != null && p.isString();
    }

    public EntityInterfaceImpl attachHandleToEntities(boolean f)
    {
        attachHandleFlag = f;
        return this;
    }

    public boolean attachHandleToEntities()
    {
        return attachHandleFlag;
    }

    public EntityInterfaceImpl allowEntitiesInImmutableValues(boolean f)
    {
        allowEntitiesInValuesFlag = f;
        return this;
    }

    public boolean allowEntitiesInImmutableValues()
    {
        return allowEntitiesInValuesFlag;
    }

    public String primaryKey()
    {
        return primaryKey;
    }

    public EntityInterfaceImpl primaryKey(String primaryKey)
    {
        this.primaryKey = primaryKey;
        return this;
    }

    public HGHandle lookupEntity(HyperNodeJson node, Json entity)
    {
        if (primaryKey != null && entity.has("primaryKey"))
        {
            HGSearchResult<HGHandle> rs = node.find(Json.object("entity", entity.at("entity"), 
                                                                primaryKey, entity.at(primaryKey)));
            try { return (rs.hasNext()) ? rs.next() : null; } finally { rs.close(); }
        }
        else
            return null;
    }
}