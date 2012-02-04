package mjson.hgdb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import mjson.Json;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGLink;

public class ArrayJsonHGDB extends Json implements HGLink
{
    ArrayList<HGHandle> targets = new ArrayList<HGHandle>();
    ArrayList<Json> L = new ArrayList<Json>();
    
    public ArrayJsonHGDB()
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
    
    @Override
    public List<Json> asJsonList() { return L; }
    public List<Object> asList() 
    {
        ArrayList<Object> A = new ArrayList<Object>();
        for (Json x: L)
            A.add(x.getValue());
        return A; 
    }
    public Object getValue() { return asList(); }
    public boolean isArray() { return true; }
    public Json at(int index) { return L.get(index); }
    public Json add(Json el) 
    { 
        L.add(el); 
        el.attachTo(this); 
        return this; 
    }
    public Json remove(Json el) 
    { 
        L.remove(el); 
        el.attachTo(null); 
        return this; 
    }

    public Json with(Json object) 
    {
        if (!object.isArray())
            throw new UnsupportedOperationException();
        // what about "enclosing" here? we don't have a provision where a Json 
        // element belongs to more than one enclosing elements...
        L.addAll(((ArrayJsonHGDB)object).L); 
        return this;
    }
    
    public Json atDel(int index) 
    { 
        Json el = L.remove(index); 
        if (el != null) 
            el.attachTo(null); 
        return el; 
    }
    
    public Json delAt(int index) 
    { 
        Json el = L.remove(index); 
        if (el != null) 
            el.attachTo(null); 
        return this; 
    }
    
    public String toString()
    {
        StringBuilder sb = new StringBuilder("[");
        for (Iterator<Json> i = L.iterator(); i.hasNext(); )
        {
            sb.append(i.next().toString());
            if (i.hasNext())
                sb.append(",");
        }
        sb.append("]");
        return sb.toString();
    }
    public int hashCode() { return L.hashCode(); }
    public boolean equals(Object x)
    {           
        return x instanceof ArrayJsonHGDB && ((ArrayJsonHGDB)x).L.equals(L); 
    }           
}