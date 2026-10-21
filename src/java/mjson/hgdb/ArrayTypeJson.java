package mjson.hgdb;

import java.util.ArrayList;
import java.util.List;

import mjson.Json;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGValueLink;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.type.HGAtomTypeBase;

public class ArrayTypeJson extends HGAtomTypeBase
{
    private HyperNodeJson node;

    public void setHyperNodeJson(HyperNodeJson node)
    {
        this.node = node;
    }
	
    public Object make(HGPersistentHandle handle,
                       LazyRef<HGHandle[]> targetSet,
                       IncidenceSetRef incidenceSet)
    {
        HGHandle [] targets = targetSet.deref();
        List<Json> elements = new ArrayList<Json>();
        for (int i = 0; i < targets.length; i++)
        {
            Object x = graph.get(targets[i]);
            Json value;
            if (x instanceof HGValueLink)
            {
                value = (Json)((HGValueLink)x).getValue();
                if (value.isObject() && node.getEntityInterface().isEntity(value))
                	value = node.getEntityInterface().createEntityReference(node, targets[i]);
            }
            else
                value = x instanceof Json ? (Json)x : Json.make(x);
            elements.add(value);
        }
        return new FrozenArrayJson(elements);
    }

    public HGPersistentHandle store(Object instance)
    {
        return graph.getHandleFactory().nullHandle();
    }

    public void release(HGPersistentHandle handle)
    {
    }
}