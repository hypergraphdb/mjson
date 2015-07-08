package mjson.hgdb;

import mjson.Json;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGValueLink;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.type.HGAtomTypeBase;

public class ObjectTypeJson extends HGAtomTypeBase
{
    private HyperNodeJson node;

    public void setHyperNodeJson(HyperNodeJson node)
    {
        this.node = node;
    }

    /**
     * This will eagerly construct a full Json object graph regardless
     * of what is being nested into what. There are options to introduce
     * laziness here: (1) whenever a property value is not a primitive
     * or (2) whenever a property value is recognized as an entity by
     * the user provided entityPredicate in the HyperNodeJson instance. 
     * A similar laziness could be implemented for array as well, except
     * it makes a little less sense there. 
     *
     *
     * The reason everything is eagerly initialized here is that most JSON
     * business structures are comparatively small and there's a lot of caching
     * of JSON properties.
     */
    public Object make(HGPersistentHandle handle,
                       LazyRef<HGHandle[]> targetSet,
                       IncidenceSetRef incidenceSet)
    {
        HGHandle [] targets = targetSet.deref();
        Json j = Json.object();
        for (HGHandle t : targets)
        {
            JsonProperty prop = graph.get(t);
            String name = graph.get(prop.getName());
            Json value = null;            
            Object x = graph.get(prop.getValue());
            if (x instanceof HGValueLink)
                value = (Json)((HGValueLink)x).getValue();
            else
                value = (Json)x;         
            if (value.isObject() && node.getEntityInterface().isEntity(value))
                value.set("hghandle", prop.getValue().getPersistent().toString());
            j.set(name, value);
        }
        return j;
    }

    public HGPersistentHandle store(Object instance)
    {
        return graph.getHandleFactory().nullHandle();
    }

    public void release(HGPersistentHandle handle)
    {
        // nothing to do
    }
}