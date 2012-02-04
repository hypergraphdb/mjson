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
            if (value.isObject())
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