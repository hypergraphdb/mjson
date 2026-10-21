package mjson.hgdb;

import java.util.HashMap;
import java.util.Map;

import mjson.Json;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.util.Mapping;

/**
 * <p>
 * Collection of static utility methods in use by this module.
 * </p>
 * 
 * @author borislav
 *
 */
public class Helpers
{
    public static Json traverse(Json j, Mapping<Json, Boolean> f)
    {
        if (!f.eval(j)) return j;
        else if (j.isObject())
        {
            for (Json x : j.asJsonMap().values())
                traverse(x, f);
        }
        else if (j.isArray())
        {
            for (Json x : j.asJsonList())
                traverse(x, f);
        }
        return j;
    }

    /**
     * <p>
     * Return a copy of <code>top</code> in which entity references are replaced by copies of the
     * entities they refer to, recursively. Each entity is resolved at its first occurrence only, so
     * cycles between entities end in references. The stored JSON (cached entities and values) is not
     * changed.
     * </p>
     */
    public static Json resolveEntities(final HyperNodeJson node, final Json top)
    {
    	final Map<HGHandle, Json> done = new HashMap<HGHandle, Json>(); 
    	return traverse(top.dup(), new Mapping<Json, Boolean>() {
    		public Boolean eval(Json j)
    		{    		
    			if (j.isObject()) 
    			{
    				Json resolved = Json.object();    				
    				for (String name : j.asJsonMap().keySet())
	    			{
	    				Json value = j.at(name);
	    				HGHandle entityHandle = node.getEntityInterface().entityReferenceToHandle(node, value);
	    				if (entityHandle != null && !done.containsKey(entityHandle.getPersistent()))
	    				{
	    					Json entity = node.get(entityHandle);
	    					value = entity == null ? null : entity.dup();
	    					done.put(entityHandle.getPersistent(), value);
	    					if (value != null)
	    						resolved.set(name, value);
	    				}
	    			}
    				j.with(resolved);
    			}
    			return true;
    		}
    	});
    }
}