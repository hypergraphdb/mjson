package mjson.hgdb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import mjson.Json;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGValueLink;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HyperNode;
import org.hypergraphdb.IncidenceSet;
import org.hypergraphdb.handle.HGLiveHandle;
import org.hypergraphdb.query.And;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.transaction.TxCacheMap;
import org.hypergraphdb.util.ArrayBasedSet;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.WeakIdentityHashMap;

/**
 * <p>
 * Represents a view of the JSON storage within a {@link HyperGraph} instance.
 * </p>
 * 
 * @author borislav
 *
 */
public class HyperNodeJson implements HyperNode
{
    private HyperGraph graph;
    // A separate atom cache is needed here because of the "auto-boxing" of
    // Json instances into HGValueLinks. The HGDB cache itself only keeps the
    // HGValueLink instances.
    private TxCacheMap<Object, HGLiveHandle> atomsTx = null;

    private HGHandle nullHandle = null;
    private HGQuery<HGHandle> findName;
    private HGQuery<HGHandle> findProperty;
    private HGQuery<HGHandle> findBoolean;
    private HGQuery<HGHandle> findNumber;
    private HGQuery<HGHandle> findString;

    private HGHandle getNullHandle()
    {
    	if (nullHandle == null)
    		nullHandle = graph.findOne(hg.type(JsonTypeSchema.nullTypeHandle));
    	return nullHandle;
    }
    
    private void makeQueries()
    {
        findName = HGQuery.make(HGHandle.class, graph).compile(hg.and(hg.type(String.class), hg.eq(hg.var("name"))));
        findProperty = HGQuery.make(HGHandle.class, graph).compile(
        			hg.and(hg.type(JsonProperty.class), hg.incident(hg.var("name")), hg.incident(hg.var("value"))));
        findBoolean = HGQuery.make(HGHandle.class, graph).compile(hg.and(hg.type(JsonTypeSchema.booleanTypeHandle), 
        					hg.eq(hg.var("value"))));
        findString = HGQuery.make(HGHandle.class, graph).compile(hg.and(hg.type(JsonTypeSchema.stringTypeHandle), 
        					hg.eq(hg.var("value"))));
        findNumber = HGQuery.make(HGHandle.class, graph).compile(hg.and(hg.type(JsonTypeSchema.numberTypeHandle), 
        					hg.eq(hg.var("value"))));
    }
    
    /**
     * <p>Create a view of the JSON within the given <code>HyperGraphDB</code> instance. Note that
     * it is strongly recommended to have only one such view instantiated per database instance because
     * it maintains its own cache of <code>Json</code> atoms and multiple views may lead to inconsistencies
     * as the caches won't be automatically synchronized.
     * </p> 
     * @param graph
     */
    public HyperNodeJson(HyperGraph graph)
    {
        this.graph = graph;
        atomsTx = new TxCacheMap<Object, HGLiveHandle>(graph.getTransactionManager(), WeakIdentityHashMap.class, null);
        makeQueries();
    }
    
    /**
     * <p>
     * Find the handle of a Json value stored in the database, that exactly matches
     * the passed in Json value. The value can be anything JSON.
     * </p>
     * 
     * @param j
     * @return
     */
    public HGHandle exactly(Json j) { return match(j, true); }
    
    /**
     * <p>
     * Get a Json structure uniquely identified by the properties in the passed
     * in structure.  The value can be anything JSON. For primitive values (strings,
     * numbers, booleans, null), this method has the same behavior as the {@link #exactly(Json)}
     * method.
     * </p>
     * 
     * @param j
     * @return
     */
    public HGHandle unique(Json j) { return match(j, false); }
    
    public HGHandle ensureProperty(final String name, final Object value)
    {
        return this.graph.getTransactionManager().transact(new Callable<HGHandle>(){
            public HGHandle call()
            {
                HGHandle result = findProperty(name, value);
                if (result != null) return result;
                HGHandle nameHandle = hg.assertAtom(graph, name);
                HGHandle valueHandle = assertAtom(Json.make(value));
                return graph.add(new JsonProperty(nameHandle, valueHandle));
            }
        });
    }
    
    public HGHandle findProperty(String name, Object value)
    {
        return findProperty(name, Json.make(value));
    }
    
    public HGHandle findProperty(String name, Json value)
    {
        HGHandle h = match(value, true);
        return h == null ? null : findProperty(name, h);
    }
    
    public HGHandle findProperty(String name, HGHandle value)
    {
        HGHandle h = findName.var("name", name).findOne();
        return h == null ? null : findProperty(h, value); 
    }
    
    public HGHandle findProperty(HGHandle name, Json value)
    {
        HGHandle h = match(value, true);
        return h == null ? null : findProperty(name, h);        
    }
    
    public HGHandle findProperty(HGHandle name, HGHandle value)
    {
        return findProperty.var("name", name).var("value", value).findOne();        
    }
    
    @SuppressWarnings("unchecked")
    public List<HGHandle> findPropertyValues(String name)
    {
        HGHandle h = findName.var("name", name).findOne();
        return h == null ? Collections.EMPTY_LIST : findPropertyValues(h);         
    }
    
    public List<HGHandle> findPropertyValues(HGHandle name)
    {
        return graph.findAll(hg.apply(hg.targetAt(graph, 1), 
                              hg.and(hg.type(JsonProperty.class), 
                               hg.orderedLink(name, hg.anyHandle()))));
    }
    
    public Json retrieve(Json j)
    {
        HGHandle h = match(j, false);
        return h == null ? null : (Json)get(h);
    }
    
    /**
     * <p>
     * Find the first <code>Json</code> element that matches the passed in <code>Json</code>
     * pattern.
     * </p>
     * 
     * @param j The <code>Json</code> pattern to match.
     * @param exact Whether this should be an exact or approximate match. Approximate means only
     * some of the properties must be there. This parameter applies recursively
     * to nested structures. Note that arrays are always matched exactly.
     * @return
     */
    public HGHandle match(Json j, boolean exact)
    {
        HGHandle h = getHandle(j);
        if (h != null)
            return h;
        if (j.isNull())
        {
            h = getNullHandle();
        }
        else if (j.isBoolean())
        {
            h = findBoolean.var("value", j).findOne();
        }
        else if (j.isString())
        {
            h = findString.var("value", j.asString()).findOne();
        }
        else if (j.isNumber())
        {
            h = findNumber.var("value", j.asDouble()).findOne();            		
        }
        else if (j.isArray())
        {
            HGHandle [] A = new HGHandle[j.asJsonList().size()];
            for (int i = 0; i < A.length; i++)
            {
                HGHandle x = match(j.at(i), exact);
                if (x == null)
                {
                    A = null;
                    break;
                }
                A[i] = x;
            }
            if (A != null)
                h = graph.findOne(hg.and(hg.type(JsonTypeSchema.arrayTypeHandle),hg.orderedLink(A)));
        }
        else if (j.isObject())
        {
            HGHandle [] A = new HGHandle[j.asJsonMap().size()];
            int i = 0;
            for (Map.Entry<String, Json> e : j.asJsonMap().entrySet())
            {
                HGHandle propHandle = findProperty(e.getKey(), e.getValue());
                if (propHandle == null)
                {
                    A = null;
                    break;
                }
                A[i++] = propHandle;
            }
            if (A != null)
            {
                And and = hg.and(hg.type(JsonTypeSchema.objectTypeHandle), hg.link(A));
                if (exact) 
                    and.add(hg.arity(i));                
                h = graph.findOne(and);
            }
        }
        return h;
    }

    public HGSearchResult<HGHandle> find(Json pattern)
    {
        return find(pattern, false);
    }
    
    @SuppressWarnings("unchecked")
    public HGSearchResult<HGHandle> find(Json pattern, boolean exact)
    {
        if (pattern.isNull())
        {
            return new ArrayBasedSet<HGHandle>(new HGHandle[] { getNullHandle()}).getSearchResult();
        }
        else if (pattern.isBoolean())
        {
            return findBoolean.var("value", pattern).execute();
        }
        else if (pattern.isString())
        {
            return findString.var("value", pattern.asString()).execute();
        }
        else if (pattern.isNumber())
        {
            return findNumber.var("value", pattern.asDouble()).execute();
        }
        else if (pattern.isArray())
        {
            HGHandle [] A = new HGHandle[pattern.asJsonList().size()];
            for (int i = 0; i < A.length; i++)
            {
                HGHandle x = match(pattern.at(i), exact);
                if (x == null)
                {
                    A = null;
                    break;
                }
                A[i] = x;
            }
            if (A != null)
                return graph.find(hg.and(hg.type(JsonTypeSchema.arrayTypeHandle),hg.orderedLink(A)));
            else
                return (HGSearchResult<HGHandle>) HGSearchResult.EMPTY;
        }
        else if (pattern.isObject())
        {
            HGHandle [] A = new HGHandle[pattern.asJsonMap().size()];
            int i = 0;
            for (Map.Entry<String, Json> e : pattern.asJsonMap().entrySet())
            {
                HGHandle propHandle = findProperty(e.getKey(), e.getValue());                
                if (propHandle == null)
                {
                    A = null;
                    break;
                }
                A[i++] = propHandle;
            }
            if (A != null)
            {
                And and = hg.and(hg.type(JsonTypeSchema.objectTypeHandle), hg.link(A));
                if (exact) 
                    and.add(hg.arity(i));                
                return graph.find(and);
            }
            else
                return (HGSearchResult<HGHandle>) HGSearchResult.EMPTY;
        }        
        else
            throw new IllegalArgumentException("Unknown JSON type: " + pattern);
    }
  
    public List<HGHandle> findAll(Json pattern)
    {
        ArrayList<HGHandle> L = new ArrayList<HGHandle>();
        HGSearchResult<HGHandle> rs = find(pattern);
        try
        {
            while (rs.hasNext())
                L.add(rs.next());
        }
        finally 
        {
            HGUtils.closeNoException(rs);
        }
        return L;
    }
    
    public Json getAll(Json pattern)
    {
        Json L = Json.array();
        HGSearchResult<HGHandle> rs = find(pattern);
        try
        {
            while (rs.hasNext())
                L.add((Json)get(rs.next()));
        }
        finally
        {
            HGUtils.closeNoException(rs);
        }
        return L;        
    }
    
    public HGHandle getHandle(Object atom)
    {
        HGHandle h = atomsTx.get(atom);
        return h == null ? graph.getHandle(atom) : h;
    }
    
    @SuppressWarnings("unchecked")
    public <T> T get(final HGHandle handle)
    {
        return graph.getTransactionManager().ensureTransaction(new Callable<T>() {
            public T call()
            {
                Object x = graph.get(handle);
                if (x instanceof HGValueLink)
                {
                    Object y = ((HGValueLink)x).getValue();
                    if (y instanceof Json)
                    {
                        x = y;
                        atomsTx.load(x, graph.getCache().get(handle.getPersistent()));
                        if (((Json) y).isObject())
                            ((Json)y).set("hghandle", handle.getPersistent().toString());
                    }
                }
                return (T)x;
            }
        });
    }

    /**
     * <p>
     * Add a new atom. If the atom is a Json compound (array or object), first assert all
     * its elements/properties. That is, while the {@link #add(Object)} method will add a new atom for
     * the top-level structure as well as for all nested structures and while the {@link HyperNodeJson#assertAtom(Object)}
     * method will always first lookup if there's a matching atom with the same value, this method
     * makes sure the top-level structure is a new atom, always, while nested properties or elements
     * are still <em>asserted</em> (i.e. added only if not already in the database). 
     * </p>
     * 
     * @param atom
     * @return
     */
    public HGHandle addTopLevel(Object atom)
    {
        if (! (atom instanceof Json))
            return graph.add(atom);
        Json j = (Json)atom;
        if (j.isObject())
        {
            HGHandle [] A = new HGHandle[j.asJsonMap().size()];
            int i = 0;
            for (Map.Entry<String, Json> e : j.asJsonMap().entrySet())
            {
                HGHandle nameHandle = findName.var("name", e.getKey()).findOne();                
                if (nameHandle == null)
                {
                	//System.out.println("name " + e.getKey() + " not found");
                    nameHandle = graph.add(e.getKey());
                }
                HGHandle valueHandle = match(e.getValue(), true);
                if (valueHandle == null)
                    valueHandle = assertAtom(e.getValue());
                HGHandle propHandle =  findProperty.var("name", nameHandle).var("value", valueHandle).findOne();                		
                if (propHandle == null)
                {
                	//System.out.println("prop " + graph.get(nameHandle) + "=" + graph.get(valueHandle) + " not found");
                    propHandle = graph.add(new JsonProperty(nameHandle, valueHandle));
                }
                A[i++] = propHandle;
            }
            return graph.add(new HGValueLink(j, A), JsonTypeSchema.objectTypeHandle);            
        }
        else if (j.isArray())
        {
            HGHandle [] A = new HGHandle[j.asJsonList().size()];
            for (int i = 0; i < A.length; i++)
            {
                HGHandle x = match(j.at(i), true);
                if (x == null)
                    x = assertAtom(j.at(i));
                A[i] = x;
            }
            return graph.add(new HGValueLink(j, A), JsonTypeSchema.arrayTypeHandle);                       
        }
        else
            return add(j);
    }
    
    public HGHandle assertAtom(Object atom)
    {
        if (! (atom instanceof Json))
            return graph.add(atom);
        Json j = (Json)atom;
        HGHandle h = getHandle(j);
        if (h != null)
            return h;
        else if (j.isNull())
        {
            h = getNullHandle();
            if (h == null)
                h = graph.add(j, JsonTypeSchema.nullTypeHandle);            
        }
        else if (j.isBoolean())
        {
            h = findBoolean.var("value", j).findOne();
            if (h == null)
                h = graph.add(j, JsonTypeSchema.booleanTypeHandle);            
        }
        else if (j.isString())
        {
            h = findString.var("value", j.asString()).findOne();
            if (h == null)
                h = graph.add(j, JsonTypeSchema.stringTypeHandle);            
        }
        else if (j.isNumber())
        {
            h = findNumber.var("value", j.asDouble()).findOne();
            if (h == null)
                h = graph.add(j, JsonTypeSchema.numberTypeHandle);            
        }
        else if (j.isArray())
        {
            HGHandle [] A = new HGHandle[j.asJsonList().size()];
            for (int i = 0; i < A.length; i++)
            {
                HGHandle x = match(j.at(i), true);
                if (x == null)
                    x = assertAtom(j.at(i));
                A[i] = x;
            }
            h = hg.findOne(graph, hg.and(hg.type(JsonTypeSchema.arrayTypeHandle),hg.orderedLink(A)));
            if (h == null)
                h = graph.add(new HGValueLink(j, A), JsonTypeSchema.arrayTypeHandle);            
        }
        else if (j.isObject())
        {
            HGHandle [] A = new HGHandle[j.asJsonMap().size()];
            int i = 0;
            for (Map.Entry<String, Json> e : j.asJsonMap().entrySet())
            {
                HGHandle nameHandle = hg.findOne(graph, hg.eq(e.getKey()));
                if (nameHandle == null)
                    nameHandle = graph.add(e.getKey());
                HGHandle valueHandle = match(e.getValue(), true);
                if (valueHandle == null)
                    valueHandle = assertAtom(e.getValue());
                HGHandle propHandle = hg.findOne(graph, hg.and(hg.type(JsonProperty.class), 
                                                               hg.link(nameHandle, 
                                                                       valueHandle)));
                if (propHandle == null)
                    propHandle = graph.add(new JsonProperty(nameHandle, valueHandle));
                A[i++] = propHandle;
            }
            h = hg.findOne(graph, hg.and(hg.type(JsonTypeSchema.objectTypeHandle), 
                                         hg.link(A), 
                                         hg.arity(i)));
            if (h == null)
                h = graph.add(new HGValueLink(j, A), JsonTypeSchema.objectTypeHandle);
        }
        return h;
    }

    public HGHandle add(Object atom)
    {
        if (! (atom instanceof Json))
            return graph.add(atom);
        Json j = (Json)atom;
        if (j.isNull())
        {
            return graph.add(j, JsonTypeSchema.nullTypeHandle);            
        }
        else if (j.isBoolean())
        {
            return graph.add(j, JsonTypeSchema.booleanTypeHandle);            
        }
        else if (j.isString())
        {
            return graph.add(j, JsonTypeSchema.stringTypeHandle);            
        }
        else if (j.isNumber())
        {
            return graph.add(j, JsonTypeSchema.numberTypeHandle);            
        }
        else if (j.isArray())
        {
            int length = j.asJsonList().size();
            //List<HGHandle> L = ((ArrayJsonHGDB)j).targets;
            //L.clear();
            HGHandle [] A = new HGHandle[length];
            for (int i = 0; i < length; i++)
            {
                HGHandle x = getHandle(j.at(i));
                if (x == null)
                    x = add(j.at(i));
                //L.add(x);
                A[i] = x;
            }            
            return graph.add(new HGValueLink(j, A), JsonTypeSchema.arrayTypeHandle);            
        }
        else if (j.isObject())
        {
            HGHandle [] A = new HGHandle[j.asJsonMap().size()];
            int i = 0;
            for (Map.Entry<String, Json> e : j.asJsonMap().entrySet())
            {
                HGHandle nameHandle = hg.findOne(graph, hg.eq(e.getKey()));
                if (nameHandle == null)
                    nameHandle = graph.add(e.getKey());
                HGHandle valueHandle = getHandle(e.getValue());
                if (valueHandle == null)
                    valueHandle = add(e.getValue());
                HGHandle propHandle = hg.findOne(graph, hg.and(hg.type(JsonProperty.class), 
                                                               hg.link(nameHandle,
                                                                       valueHandle)));
                if (propHandle == null)
                    propHandle = graph.add(new JsonProperty(nameHandle, valueHandle));
                A[i++] = propHandle;
            }
            return graph.add(new HGValueLink(j, A), JsonTypeSchema.objectTypeHandle);
        }
        throw new IllegalArgumentException();
    }
    
    public HGHandle add(Object atom, HGHandle type, int flags)
    {
        return graph.add(atom, type, flags);
    }

    public void define(HGHandle handle, HGHandle type, Object instance, int flags)
    {
        graph.define(handle, type, instance, flags);
    }

    public boolean remove(HGHandle handle)
    {
        return graph.remove(handle);
    }

    public boolean update(Object atom)
    {
        HGHandle h = getHandle(atom);
        if (h == null)
            throw new HGException("Could not find handle for atom " + atom);
        else
            return replace(h, atom, getType(h));
    }
    
    public boolean replace(final HGHandle handle, final Object newValue, final HGHandle newType)
    {
        return graph.getTransactionManager().ensureTransaction(new Callable<Boolean>() {
           public Boolean call() { return replaceTransaction(handle, newValue, newType); }
        });
    }
    
    private boolean replaceTransaction(HGHandle handle, Object newValue, HGHandle newType)
    {
        if (! (newValue instanceof Json))
            return graph.replace(handle, newValue, newType);
        Object currentValue = get(handle);
        if (! (currentValue instanceof Json))
            return graph.replace(handle, newValue, newType);
        Json j = (Json)currentValue;
        if (j.isPrimitive() || j.isNull())
            throw new IllegalArgumentException("Refusing to replace JSON primitive atoms. " +
                                               "Use HyperGraph's own replace method if you know what you're doing.");
        HGHandle currentType = getType(handle);
        if (!currentType.equals(newType) ) // this is outside of the scope of what we're doing with JSON
            return graph.replace(handle, newValue, newType);

        Json h = (Json)newValue;        
        // Ok, here we are replacing a JSON object with a JSON object or a JSON array with a JSON array.
        
        // For arrays, we just make sure we update each of its elements and set the new target set.
        if (h.isArray())
        {
            HGHandle [] targets = new HGHandle[h.asJsonList().size()];
            for (int i = 0; i < targets.length; i++)
            {
                Json el = h.at(i);
                if (el.isPrimitive() || el.isNull())
                    targets[i] = this.assertAtom(el);
                else if (el.isArray())
                {
                    // This is a bit tricky because we need to update the elements of 
                    // nested array when those are objects. But the array nesting depth
                    // is arbitrary and we don't know how to update nested arrays because
                    // we don't have their handles. So for now we don't and we assume
                    // that nesting arrays within arrays is not used in this context.
                    targets[i] = this.assertAtom(el);
                }
                else
                {
                    // if the object has a handle already, we recursively update/replace
                    // otherwise we assert it.
                    if (el.has("hghandle"))
                    {
                        targets[i] = graph.getHandleFactory().makeHandle(el.at("hghandle").asString());
                        replace(targets[i], el, JsonTypeSchema.objectTypeHandle);
                    }
                    else
                        targets[i] = this.assertAtom(el);
                }                    
            }
            // Here we may need to loop through the old target set and maybe do some cleanup...
            return graph.replace(handle, new HGValueLink(h, targets), JsonTypeSchema.arrayTypeHandle);
        }
        
        // For objects, we sync up old with new, deleting missing properties and setting new ones.
        
        // First put all properties of the old object in a map so we know their value handles:
        HGValueLink currentAsLink = graph.get(handle);
        Map<String, HGHandle> valueMap = new HashMap<String, HGHandle>();
        for (HGHandle propHandle : currentAsLink)
        {
            JsonProperty prop = graph.get(propHandle);
            valueMap.put(graph.get(prop.getName()).toString(), prop.getValue());
        }
        
        HGHandle [] A = new HGHandle[h.asJsonMap().size()];
        int i = 0;
        for (Map.Entry<String, Json> e : h.asJsonMap().entrySet())
        {
            HGHandle nameHandle = hg.findOne(graph, hg.eq(e.getKey()));
            if (nameHandle == null)
                nameHandle = graph.add(e.getKey());
            HGHandle valueHandle = null;
            Json el = e.getValue();
            if (el.isPrimitive() || el.isNull())
                valueHandle = this.assertAtom(el);            
            else if (el.isArray())
            {
                // here we replace the old array if there's one
                valueHandle = valueMap.get(e.getKey());
                if (valueHandle != null)
                    replace(valueHandle, el, JsonTypeSchema.arrayTypeHandle);
                else
                    valueHandle = this.assertAtom(el);
            }
            else
            {
                if (el.has("hghandle"))
                {
                    valueHandle = graph.getHandleFactory().makeHandle(el.at("hghandle").asString());
                    replace(valueHandle, el, JsonTypeSchema.objectTypeHandle);
                }
                else if (valueMap.containsKey(e.getKey()))
                {
                    valueHandle = valueMap.get(e.getKey());
                    replace(valueHandle, el, JsonTypeSchema.objectTypeHandle);
                }
                else
                    valueHandle = this.assertAtom(el);
            }
            HGHandle propHandle = hg.findOne(graph, hg.and(hg.type(JsonProperty.class), 
                                                           hg.link(nameHandle,
                                                                   valueHandle)));
            if (propHandle == null)
                propHandle = graph.add(new JsonProperty(nameHandle, valueHandle));
            A[i++] = propHandle;            
        }
        return graph.replace(handle, new HGValueLink(h, A), JsonTypeSchema.objectTypeHandle);
    }

    public HGHandle getType(HGHandle handle)
    {
        return graph.getType(handle);
    }

    public IncidenceSet getIncidenceSet(HGHandle handle)
    {
        return graph.getIncidenceSet(handle);
    }

    public <T> T findOne(HGQueryCondition condition)
    {
        return graph.findOne(condition);
    }

    public <T> HGSearchResult<T> find(HGQueryCondition condition)
    {
        return graph.find(condition);
    }

    public <T> T getOne(HGQueryCondition condition)
    {
        return get((HGHandle)findOne(condition));
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> getAll(HGQueryCondition condition)
    {
        List<T> L = new ArrayList<T>();
        for (HGHandle h : findAll(condition))
            L.add((T)get(h));
        return L;
    }

    @SuppressWarnings("unchecked")
    public List<HGHandle> findAll(HGQueryCondition condition)
    {
        return graph.findAll(condition);
    }

    public long count(HGQueryCondition condition)
    {
        return graph.count(condition);
    }
}