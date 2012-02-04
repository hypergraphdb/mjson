package mjson.hgdb;

import mjson.Json;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.type.javaprimitive.DoubleType;

public class NumberTypeJson extends DoubleType
{
    public Object make(HGPersistentHandle handle,
                       LazyRef<HGHandle[]> targetSet,
                       IncidenceSetRef incidenceSet)
    {
        return Json.make(super.make(handle, targetSet, incidenceSet));
    }

    public HGPersistentHandle store(Object instance)
    {
        Json j = (Json)instance;
        if (!j.isNumber())
            throw new IllegalArgumentException();        
        return super.store(j.asDouble());
    }
}