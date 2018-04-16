package mjson.hgdb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import mjson.Json;
import mjson.hgdb.querying.CrossProductResultSet;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGSystemFlags;
import org.hypergraphdb.HGValueLink;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HyperNode;
import org.hypergraphdb.IncidenceSet;
import org.hypergraphdb.handle.HGLiveHandle;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.query.impl.HandleArrayResultSet;
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
    HyperGraph graph;
    
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
    private EntityInterface entityInterface = new EntityInterfaceImpl();

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
           hg.and(hg.type(graph.getTypeSystem().getTypeHandle(JsonProperty.class)), 
                  hg.incident(hg.var("name")), 
                  hg.incident(hg.var("value"))));
        findBoolean = HGQuery.make(HGHandle.class, graph).compile(hg.and(hg.type(JsonTypeSchema.booleanTypeHandle), 
        					hg.eq(hg.var("value"))));
        findString = HGQuery.make(HGHandle.class, graph).compile(hg.and(hg.type(JsonTypeSchema.stringTypeHandle), 
        					hg.eq(hg.var("value"))));
        findNumber = HGQuery.make(HGHandle.class, graph).compile(hg.and(hg.type(JsonTypeSchema.numberTypeHandle), 
        					hg.eq(hg.var("value"))));
        ObjectTypeJson objectType = graph.get(JsonTypeSchema.objectTypeHandle);
        objectType.setHyperNodeJson(this);
        ArrayTypeJson arrayType = graph.get(JsonTypeSchema.arrayTypeHandle);
        arrayType.setHyperNodeJson(this);
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
     * <p>Return the underlying graph.</p>
     */
    public HyperGraph graph()
    {
    	return graph;
    }
    
    /**
     * <p>Return the {@link EntityInterface} implementation this node is using.</p>
     */
    public EntityInterface getEntityInterface()
    {
        return entityInterface;
    }

    /**
     * <p>Configure the {@link EntityInterface} for use by thisnode. It must be a valid
     * implementation, not a null value.</p>
     */
    public HyperNodeJson setEntityInterface(EntityInterface entityInterface)
    {
        entityInterface.entityHandleProperty(); // check for null entityInterface
        this.entityInterface = entityInterface;
        return this;
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
                HGHandle valueHandle = add(Json.make(value));
                return graph.add(new JsonProperty(nameHandle, valueHandle));
            }
        });
    }
    
    
    public HGSearchResult<HGHandle> findPropertyPattern(String namePattern, Object valuePattern)
    {
    	HGHandle h = findName.var("name", namePattern).findOne();	
    	if (h == null)
    		return HGJsonQuery.empty();
    	HGSearchResult<HGHandle> names = new HandleArrayResultSet(new HGHandle[] { h } );
    	HGSearchResult<HGHandle> values = this.find(Json.make(valuePattern));
    	CrossProductResultSet<HGHandle> namesCrossValues = new CrossProductResultSet<HGHandle>(names, values);    	
    	// Query a JSON property based on a cross-product result of name, value pair.    	
    	return HGJsonQuery.pipeCrossProductToCompiledQuery(namesCrossValues, findProperty, "name", "value");
    }
    
    public HGHandle findProperty(String name, Object value)
    {
        return findProperty(name, Json.make(value));
    }
    
    public HGHandle findProperty(String name, Json value)
    {
    	return findProperty(name, value, true);
    }

    public HGHandle findProperty(String name, Json value, boolean exact)
    {
        HGHandle h = match(value, exact);
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
     * @return The <code>HGHandle</code> of the first matched element or <code>null</code>
     * if none was found.
     */
    public HGHandle match(Json j, boolean exact)
    {
        HGHandle h = getHandle(j);
        if (h != null)
            return h;
        try (HGSearchResult<HGHandle> rs = this.find(j, exact)) 
        {
        	return rs.hasNext() ? rs.next() : null;
        }
    }

    /**
     * <p>Return {@link #find(Json, boolean)} with <code>false</code> exact argument.</p>
     */
    public HGSearchResult<HGHandle> find(Json pattern)
    {
        return find(pattern, false);
    }
    
    /**
     * <p>
     * Find all atoms that match the specified JSON pattern. A pattern is any JSON
     * structure, from a primitive type to a deeply nested object. This method will
     * attempt to find all atoms that have the same form. Here is what having the same
     * form means:
     * </p>
     * <ul>
     * <li>A JSON primitive (boolean, number, string or null) must be the same type and have the same value.</li>
     * <li>A JSON array must be of the same length as <code>pattern</code> and each of its
     * elements must match at the corresponding positions. So arrays are always strictly matched</li>
     * <li>A JSON object must have all that properties that <code>pattern</code> and each of their
     * values must match. If the <code>exact</code> parameter is <code>true</code>, then the 
     * matching atom may not have any extra properties, but it must match <code>pattern</code> exactly.</li>
     * </ul>
     * @param pattern
     * @param exact
     * @return
     */
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
        	// TODO - paternize
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
            	if (exact)
            		return graph.find(hg.and(hg.type(JsonTypeSchema.arrayTypeHandle), hg.orderedLink(A), hg.arity(A.length)));
            	else
            		return graph.find(hg.and(hg.type(JsonTypeSchema.arrayTypeHandle),hg.orderedLink(A)));
            else
                return (HGSearchResult<HGHandle>) HGSearchResult.EMPTY;
        }
        else if (pattern.isObject())
        {
            return HGJsonQuery.findObjectPattern(this, pattern, exact);
        }        
        else
            throw new IllegalArgumentException("Unknown JSON type: " + pattern);
    }
  
    /**
     * <p>
     * Collect all results from {@link #find(Json)} into a Java <code>List</code> and
     * close the result set properly.
     * </p>
     * 
     * @param pattern The Json pattern to use. See {@link #find(Json)}.
     */
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
        	rs.close();
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
                        Json j = (Json)y;
                        atomsTx.load(j, graph.getCache().get(handle.getPersistent()));
                        if (j.isObject() && entityInterface.isEntity(j) && entityInterface.entityHandleProperty() != null)
                            j.set(entityInterface.entityHandleProperty(), handle.getPersistent().toString());
                        return (T)j;
                    }
                }
                return (T)x;
            }
        });
    }

    /**
     * <p>Adding an atom to the JSON node, or the underlying graph. If the atom is not
     * an instance of <code>JSON</code>, the operation is delegated to the {@link HyperGraph}
     * instance. Otherwise, what happens exactly depends on the configuration of {@link mjson.Json} entity
     * management. Refer to the {@link EntityInterface} for more information on the relevant options.
     * What follows applies to <code>Json</code> typed atoms only.
     * </p>
     *
     * <p>
     * If the atom is an element already in the data store its handle is returned without anything
     * else happening. An atom is deemed in the store either if its Java reference is in the 
     * cache or if it's a <code>Json</code> object carrying a <code>hghandle</code> property. If 
     * you want to add multiple copies of a given entity, use the <code>Json.dup</code> to clone
     * the entity. Otherwise, repeated calls to the <code>add</code> method will just return the
     * same <code>HGHandle</code>.
     * </p>
     * 
     * <p>
     * If the atom is not already in the store then if it's an entity (according to 
     * <code>getEntityInterface.isEntity()</code>), then it's added as a new atom. Otherwise,
     * if it's not an entity, then an <code>assert</code> operation is performed. An 
     * <code>assert</code> first looks up by value which tries to find
     * an exact match and then the value is be added only if it's not found. In either case
     * the handle of the value is returned.  
     * </p>
     *
     * <p>
     * What happens with nested JSON elements such as members of arrays or object properties?
     * If the parent <code>Json</code> is an entity, there is not inconsistency in treating
     * its nested elements with the same logic, performing a recursive add. However, if the parent
     * element is not an entity, but it has a nested entity element, then what are we to do?
     * This is answered by the more general question: can an immutable aggregate structure contain
     * a mutable part? We can say yes, sure, since the immutable aggregate, the top-level 
     * <code>Json</code> in our case is simply holding a reference, which is immutable so there's
     * inconsistency. On the other hand, we can answer "no" following the rationale that an
     * immutable structure means immutable all the way, to the deepest nesting of its values.
     * Therefore if a nested entity changes our parent value cannot claim to be immutable anymore.
     * This second choice is the more conservative and seems the more sensible position, so
     * that's what we do: if we are asserting an immutable value and in the process we encounter
     * an <code>entity</code>, we complain with an exception since we won't be able to guarantee
     * mutability in the future. This behavior can be overwritten by changing the {@link EntityInterface}
     * configuration options <code>allowEntitiesInImmutableValues</code>.
     * </p>
     */
    public HGHandle add(Object atom)
    {
        if (! (atom instanceof Json))
            return graph.add(atom);
        final Json j = (Json)atom;
        HGHandle incache = getHandle(j);
        if (incache != null)
            return incache;
    	return graph.getTransactionManager().ensureTransaction(new Callable<HGHandle>() { public HGHandle call() {
    		if (!j.isObject() || !entityInterface.isEntity(j))
    			return addImpl(j, null);    		
    		// We have to store an entity and we have several possible situations: 
            // 1. the atom is not stored in the db at all => we have to add it
            // 2. the atom is stored already and it's different => we have to replace
            // 3. the atom is stored but it's the same extensionally => we just put in cache    		
    		HGHandle h = null;
    		if (entityInterface.entityHandleProperty() != null && j.has(entityInterface.entityHandleProperty()))
    			h = graph.getHandleFactory().makeHandle(j.at(entityInterface.entityHandleProperty()).asString());
    		else
    			h = entityInterface.lookupEntity(HyperNodeJson.this, j);
    		if (h == null)
    			return addImpl(j, null);
    		Json existing = get(h);
    		if (existing == null)
    			return addImpl(j, h);
    		else if (j == existing)
    			return h;
    		else // we won't try to value compare the objects here in case they 
    			 //differ superficially due to 'handle property' present or absent
    			replace(h, j, JsonTypeSchema.objectTypeHandle);
    		return h;	
    	}});
    }

    /**
     * Add a new element to the database: a pure value is just asserted so we try to avoid
     * duplicates while an entity is added as a new separate atom.
     * @param j
     * @param handle The handle of the entity, if this is an entity, or <code>null</code> auto-generate.
     * @return The handle of the newly added element.
     */
    private HGHandle addImpl(Json j, HGHandle handle)
    {
    	HGHandle h = j.isObject() && entityInterface.isEntity(j) ? addTxn(j, handle) : assertTxn(j);
        get(h); // ensure presence in local atomTx
        return h;
    }

    /**
     * When a value is a reference to the entity, return the handle to that entity. 
     * If an entity with that handle does not exist, create one to ensure we never
     * have dangling entity references. The auto-created entity will be type-less
     * and property-less.
     */
    private HGHandle maybeEntityRef(Json value)
    {
        HGHandle h = entityInterface.entityReferenceToHandle(this, value);
        if (h != null && get(h) == null)
        	graph.define(h, JsonTypeSchema.objectTypeHandle, new HGValueLink(Json.object(), new HGHandle[0]), 0);
    	return h;
    }
    
    private HGHandle addElement(Json j, HGHandle typeHandle, HGHandle elementHandle)
    {
        if (elementHandle == null) 
        		elementHandle = graph.add(j, typeHandle);
        else
        		graph.define(elementHandle, typeHandle, j, HGSystemFlags.DEFAULT); 
    		return elementHandle;
    }
    
    private HGHandle addValue(Json value)
    {
        HGHandle valueHandle = getHandle(value);
        if (valueHandle == null)
        		valueHandle = maybeEntityRef(value);
        if (valueHandle == null)
            valueHandle = addImpl(value, null);
        return valueHandle;
    }
    
    private HGHandle addProperty(String name, Json value)
    {
    		HGHandle valueHandle = addValue(value);
        HGHandle propHandle = null;
        HGHandle nameHandle = hg.findOne(graph, hg.eq(name));
        if (nameHandle == null)
            nameHandle = graph.add(name);
        else // if the name doesn't exist, the property is definitely not there, otherwise it might
            propHandle = hg.findOne(graph, hg.and(hg.type(JsonProperty.class), 
                                                       hg.link(nameHandle,
                                                               valueHandle)));
        if (propHandle == null)
            propHandle = graph.add(new JsonProperty(nameHandle, valueHandle));
        return propHandle;
    }
    
    private HGHandle addTxn(Json j, HGHandle handle)
    {
        if (j.isNull())
        		addElement(j, JsonTypeSchema.nullTypeHandle, handle);
        else if (j.isBoolean())
        		addElement(j, JsonTypeSchema.booleanTypeHandle, handle);
        else if (j.isString())
            addElement(j, JsonTypeSchema.stringTypeHandle, handle);
        else if (j.isNumber())
            addElement(j, JsonTypeSchema.numberTypeHandle, handle);        	
        else if (j.isArray())
        {
            int length = j.asJsonList().size();
            HGHandle [] A = new HGHandle[length];
            for (int i = 0; i < length; i++)
                A[i] = addValue(j.at(i));
            if (handle == null)
            		return graph.add(new HGValueLink(j, A), JsonTypeSchema.arrayTypeHandle);
            else
            {
            		graph.define(handle, JsonTypeSchema.arrayTypeHandle, new HGValueLink(j, A), 0);
            		return handle;
            }            		
        }
        else if (j.isObject())
        {
            HGHandle thisHandle = null;
            if (entityInterface.entityHandleProperty() != null && j.has(entityInterface.entityHandleProperty())) 
                thisHandle = graph.getHandleFactory().makeHandle(j.atDel(entityInterface.entityHandleProperty()).asString());
            else if (handle != null)
            		thisHandle = handle;
            HGHandle [] A = new HGHandle[j.asJsonMap().size()];                    
            int i = 0;
            for (Map.Entry<String, Json> e : j.asJsonMap().entrySet())
            {
                A[i++] = addProperty(e.getKey(), e.getValue());
            }
            if (thisHandle != null)
            {
                graph.define(thisHandle, JsonTypeSchema.objectTypeHandle, new HGValueLink(j, A), 0);
            }
            else
            {
                thisHandle = graph.add(new HGValueLink(j, A), JsonTypeSchema.objectTypeHandle);
                if (entityInterface.entityHandleProperty() != null)
                    j.set(entityInterface.entityHandleProperty(), thisHandle.getPersistent().toString());
            }
            return thisHandle;
        }
        throw new IllegalArgumentException();
    }

    private HGHandle assertTxn(Json j)
    {
        HGHandle h = this.maybeEntityRef(j);
        if (h != null)
        	return h;
        
        if (j.isNull())
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
                Json ati = j.at(i);
                if (ati.isObject() && entityInterface.isEntity(ati))
                {
                	if (!entityInterface.allowEntitiesInImmutableValues())
	                    throw new JsonNodeException("Trying to store entity/mutable object at index " + i + 
	                                                ", from Json array.", j);
                	Json ref = entityInterface.createEntityReference(this, add(ati)); 
                	A[i] = entityInterface.entityReferenceToHandle(this, ref);
                }
                else
                	A[i] = assertTxn(ati);
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
            	Json value = e.getValue();
            	HGHandle valueHandle = null;
                if (value.isObject() && entityInterface.isEntity(value))
                {
                	if (!entityInterface.allowEntitiesInImmutableValues())
	                    throw new JsonNodeException("Trying to store entity/mutable object at property " + e.getKey() + 
	                                            ", from Json array.", j);
                	Json ref = entityInterface.createEntityReference(this, add(value)); 
                	valueHandle = entityInterface.entityReferenceToHandle(this, ref);                	
                }
                else
                	valueHandle = assertTxn(value);
                HGHandle propHandle = null;
                HGHandle nameHandle = hg.findOne(graph, hg.eq(e.getKey()));
                if (nameHandle == null)
                    nameHandle = graph.add(e.getKey());
                else
                    propHandle = hg.findOne(graph, hg.and(hg.type(JsonProperty.class), 
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

    public HGHandle add(Object atom, HGHandle type, int flags)
    {
    	// Make sure somebody doesn't get smart and use this method, which we need to provide
    	// to fully implement the HyperNode interface for JSON elements.
    	if (JsonTypeSchema.isJsonType(type))
    	{
    		throw new IllegalArgumentException("Use the HyperNodeJson.add(Object) method to add JSON elements to the database.");
    	}
        return graph.add(atom, type, flags);
    }

    /**
     * Implements the semantics of the {@link HyperNode#define(HGHandle, HGHandle, Object, int)} method. 
     * When the atom defined is a <code>Json</code> instance, the type parameter is ignored. 
     */
    public void define(final HGHandle handle, final HGHandle type, final Object instance, final int flags)
    {
    		if (! (instance instanceof Json))
    			graph.define(handle, type, instance, flags);
    		graph.getTransactionManager().ensureTransaction(new Callable<HGHandle>() { public HGHandle call() {
    			if (get(handle) != null)
    				throw new IllegalArgumentException("Cannot define a new atom since handle " + handle +  "is already used.");
    			addImpl((Json)instance, handle);    			
    			//graph.define(handle, type, instance, flags);
    			return handle;
    		} });
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
                    targets[i] = this.assertTxn(el);
                else if (el.isArray())
                {
                    // This is a bit tricky because we need to update the elements of 
                    // nested array when those are objects. But the array nesting depth
                    // is arbitrary and we don't know how to update nested arrays because
                    // we don't have their handles. So for now we don't and we assume
                    // that nesting arrays within arrays is not used in this context.
                    targets[i] = this.assertTxn(el);
                }
                else
                {
                    // if the object has a handle already, we recursively update/replace
                    // otherwise we assert it.
                    if (entityInterface.isEntity(el))
                    {
                    	Json ref = entityInterface.createEntityReference(this, add(el));
                        targets[i] = entityInterface.entityReferenceToHandle(this, ref);
                    }
                    else
                        targets[i] = this.assertTxn(el);
                }
            }
            // Here we may need to loop through the old target set and maybe do some cleanup...
            return graph.replace(handle, new HGValueLink(h, targets), JsonTypeSchema.arrayTypeHandle);
        }
        
        // For objects, we sync up old with new, deleting missing properties and setting new ones.
        
        // First put all properties of the old object in a map so we know their value handles:
        Object xx = graph.get(handle);
        if (! (xx instanceof HGValueLink) ) 
        {
        	System.out.println("Atom expected value link : " + xx);
        	xx = graph.get(handle);
        }
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
                valueHandle = this.assertTxn(el);            
            else if (el.isArray())
            {
                // here we replace the old array if there's one
                valueHandle = valueMap.get(e.getKey());
                if (valueHandle != null)
                    replace(valueHandle, el, JsonTypeSchema.arrayTypeHandle);
                else
                    valueHandle = this.assertTxn(el);
            }
            else
            {
                if (entityInterface.isEntity(el))
                {
                	Json ref = entityInterface.createEntityReference(this, add(el));
                    valueHandle = entityInterface.entityReferenceToHandle(this, ref);
                }
                else if (valueMap.containsKey(e.getKey()))
                {
                    valueHandle = valueMap.get(e.getKey());
                    replace(valueHandle, el, JsonTypeSchema.objectTypeHandle);
                }
                else
                    valueHandle = this.assertTxn(el);
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
