package mjson.hgdb;

import java.util.ArrayList;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGLink;

import mjson.Json;

public class ObjectJsonHGDB extends Json implements HGLink
{
    private ArrayList<HGHandle> targets = new ArrayList<HGHandle>();
    
    public ObjectJsonHGDB()
    {
    }
    
    public int getArity()
    {
        return targets.size();
    }

    public HGHandle getTargetAt(int i)
    {
        return targets.get(i);
    }

    public void notifyTargetHandleUpdate(int i, HGHandle handle)
    {
        targets.set(i, handle);
    }

    public void notifyTargetRemoved(int i)
    {
        targets.remove(i);
    }
}