package mjson.hgdb;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPlainLink;

public class JsonProperty extends HGPlainLink
{
    public JsonProperty(HGHandle...args)
    {
        super(args);
    }
    
    public HGHandle getName()
    {
        return getTargetAt(0);
    }

    public HGHandle getValue()
    {
        return getTargetAt(1);
    }   
}