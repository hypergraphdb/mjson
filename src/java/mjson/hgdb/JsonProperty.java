package mjson.hgdb;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPlainLink;

/**
 * <p>
 * Represents a <code>Json</code> property instance - a name/value pair where the name is a standard
 * Java <code>String</code> and the value is an arbitrary <code>Json</code> object. Both the name and
 * value are stored as separate atoms and the <code>JsonProperty</code> itself is simply a HyperGraphDB
 * link. Note that while it is possible to modify a <code>JsonProperty</code> just like it is possible
 * to modify any HyperGraphDB link, the intent behind those atoms is that they be immutable. 
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class JsonProperty extends HGPlainLink
{
    public JsonProperty(HGHandle...args)
    {
        super(args);
    }
    
    /**
     * <p>Return the handle of the name atom of this property.</p>
     */
    public HGHandle getName()
    {
        return getTargetAt(0);
    }

    /**
     * <p>Return the handle of the value atom of this property.</p>
     */
    public HGHandle getValue()
    {
        return getTargetAt(1);
    }   
}