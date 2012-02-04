package mjson.hgdb;

import mjson.Json;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.type.HGAtomTypeBase;

public class BoolTypeJson extends HGAtomTypeBase
{
    private HGPersistentHandle htrue = null, hfalse = null;
    
    @Override
    public void setHyperGraph(HyperGraph graph)
    {
        super.setHyperGraph(graph);
        // NB: the following assumes that the primitive type for Java booleans
        // DOES reuse the same value handles for true and false instead of storing
        // a different key-value record every time!
        htrue = graph.getTypeSystem().getAtomType(Boolean.class).store(true);
        hfalse = graph.getTypeSystem().getAtomType(Boolean.class).store(false);
    }
    
    public Object make(HGPersistentHandle handle,
                       LazyRef<HGHandle[]> targetSet,
                       IncidenceSetRef incidenceSet)
    {
        return htrue.equals(handle) ? Json.make(true) : Json.make(false);        
    }

    public HGPersistentHandle store(Object instance)
    {
        Json j = (Json)instance;
        if (!j.isBoolean())
            throw new IllegalArgumentException();
        return j.asBoolean() ? htrue : hfalse;
    }

    public void release(HGPersistentHandle handle)
    {        
    }
}