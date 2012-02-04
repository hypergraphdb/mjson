package mjson.hgdb;

import mjson.Json;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.type.HGAtomTypeBase;

public class NullTypeJson extends HGAtomTypeBase
{

    public Object make(HGPersistentHandle handle,
                       LazyRef<HGHandle[]> targetSet,
                       IncidenceSetRef incidenceSet)
    {
        return Json.nil();
    }

    public HGPersistentHandle store(Object instance)
    {
        Json j = (Json)instance;
        if (!j.isNull())
            throw new IllegalArgumentException();
        return graph.getHandleFactory().nullHandle();
    }

    public void release(HGPersistentHandle handle)
    {
    }
}