package mjson.hgdb;

import mjson.Json;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGValueLink;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.type.HGAtomTypeBase;

public class ArrayTypeJson extends HGAtomTypeBase
{
    public Object make(HGPersistentHandle handle,
                       LazyRef<HGHandle[]> targetSet,
                       IncidenceSetRef incidenceSet)
    {
        HGHandle [] targets = targetSet.deref();
        Json A = Json.array();
        for (int i = 0; i < targets.length; i++)
        {
            Object x = graph.get(targets[i]);
            if (x instanceof HGValueLink)
            {
                Json value = (Json)((HGValueLink)x).getValue();
                if (value.isObject())
                    value.set("hghandle", targets[i].getPersistent().toString());
                A.add(value);
            }
            else
                A.add(x);
        }
        return A;
    }

    public HGPersistentHandle store(Object instance)
    {
        return graph.getHandleFactory().nullHandle();
    }

    public void release(HGPersistentHandle handle)
    {
    }
}