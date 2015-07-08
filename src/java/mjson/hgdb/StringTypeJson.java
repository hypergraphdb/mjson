package mjson.hgdb;

import mjson.Json;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.type.javaprimitive.StringType;

public class StringTypeJson extends StringType
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
        if (!j.isString())
            throw new IllegalArgumentException();        
        return super.store(j.asString());
    }
}