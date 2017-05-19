package mjson.hgdb;

import java.util.IdentityHashMap;

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

    public static Json resolveEntities(final HyperNodeJson node, final Json top)
    {
    	final IdentityHashMap<HGHandle, Json> done = new IdentityHashMap<HGHandle, Json>(); 
    	return traverse(top, new Mapping<Json, Boolean>() {
    		public Boolean eval(Json j)
    		{    		
    			if (j.isObject()) 
    			{
    				Json resolved = Json.object();    				
    				for (String name : j.asJsonMap().keySet())
	    			{
	    				Json value = j.at(name);
	    				HGHandle entityHandle = node.getEntityInterface().entityReferenceToHandle(node, value);
	    				if (entityHandle != null)
	    				{
	    					value = done.get(entityHandle);
	    					if (value == null)
	    					{
	    						value = node.get(entityHandle);
	    						done.put(entityHandle, value);
	    						resolved.set(name, value);
	    					}
	    				}
	    			}
    				j.with(resolved);
    			}
    			return true;
    		}
    	});
    }
}