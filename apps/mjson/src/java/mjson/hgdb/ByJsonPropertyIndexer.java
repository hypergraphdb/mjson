package mjson.hgdb;

import java.util.Comparator;
import mjson.Json;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGValueLink;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.indexing.HGKeyIndexer;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.type.HGAtomType;

public class ByJsonPropertyIndexer extends HGKeyIndexer
{
    private String propertyName;
    private HGHandle propertyType;

    public ByJsonPropertyIndexer() 
    {
        
    }
    
    public ByJsonPropertyIndexer(String propertyName, HGHandle propertyType) 
    {
        this.propertyName = propertyName;
        this.propertyType = propertyType;
    }
    
    public ByteArrayConverter<?> getConverter(HyperGraph graph)
    {
        HGAtomType type = graph.get(propertyType);        
        return (ByteArrayConverter<?>)type;
    }

    public Comparator<?> getComparator(HyperGraph graph)
    {
        HGAtomType type = graph.get(propertyType);
        return (Comparator<?>)type;
    }

    public Object getKey(HyperGraph graph, Object atom)
    {
        Json j;
        if (atom instanceof Json)
            j = (Json)atom;
        else
            j = (Json)((HGValueLink)atom).getValue();        
        Json p = j.at(propertyName);
        if (p == null)
            return null;
        else if (p.isNumber())
            return p.asDouble();
        else
            return p.getValue();
    }
    
    @Override
    public void index(HyperGraph graph, HGHandle atomHandle, Object atom,
                      HGIndex index)
    {
        Object key = getKey(graph, atom);
        if (key != null)        
            index.addEntry(key, graph.getPersistentHandle(atomHandle));
    }

    @Override
    public void unindex(HyperGraph graph, HGHandle atomHandle, Object atom,
                        HGIndex index)
    {
        Object key = getKey(graph, atom);
        if (key != null)
            index.removeEntry(key, graph.getPersistentHandle(atomHandle));
    }

    public String getPropertyName()
    {
        return propertyName;
    }

    public void setPropertyName(String propertyName)
    {
        this.propertyName = propertyName;
    }

    public HGHandle getPropertyType()
    {
        return propertyType;
    }

    public void setPropertyType(HGHandle propertyType)
    {
        this.propertyType = propertyType;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((propertyName == null) ? 0 : propertyName.hashCode());
        result = prime * result
                + ((propertyType == null) ? 0 : propertyType.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ByJsonPropertyIndexer other = (ByJsonPropertyIndexer) obj;
        if (propertyName == null)
        {
            if (other.propertyName != null)
                return false;
        }
        else if (!propertyName.equals(other.propertyName))
            return false;
        if (propertyType == null)
        {
            if (other.propertyType != null)
                return false;
        }
        else if (!propertyType.equals(other.propertyType))
            return false;
        return true;
    }     
}