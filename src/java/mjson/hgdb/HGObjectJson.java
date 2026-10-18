package mjson.hgdb;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import mjson.Json;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.transaction.HGTransaction;

/**
 * <p>
 * The JSON object produced by the {@link HyperNodeJson#factory() HyperNodeJson factory}. It has two
 * modes:
 * </p>
 * <ul>
 * <li><b>Plain</b>: it behaves like a mutable mjson object. Every object an application creates
 * starts this way.</li>
 * <li><b>Entity</b>: it is the canonical runtime object of an entity stored in a
 * {@link HyperNodeJson}, the one Java object the cache holds for the entity's handle. Adding an entity
 * in plain mode promotes it in place; entities loaded from storage are created in this mode.</li>
 * </ul>
 * <p>
 * In entity mode the properties are an immutable snapshot whose values are frozen values, primitives
 * or entity references, and every change swaps in a new snapshot. So readers always see a complete
 * state, and changes made in a transaction that aborts are undone. Nested objects and arrays are
 * reached through access paths ({@link #at(String)}): changing them changes this entity only, by
 * copy-on-write, never the shared values. The entity is not isolated between threads: changes are
 * visible to other threads right away.
 * </p>
 */
public class HGObjectJson extends Json
{
    private static final long serialVersionUID = 1L;

    // Plain mode: a mutable map. Entity mode: an unmodifiable snapshot, replaced on every change.
    private volatile Map<String, Json> properties;
    private volatile HyperNodeJson node = null;
    private volatile HGHandle handle = null;
    // The top-level transaction for which a restore-on-abort action is registered.
    private HGTransaction restoreRegisteredFor = null;

    public HGObjectJson()
    {
        this.properties = new HashMap<String, Json>();
    }

    private HGObjectJson(HyperNodeJson node, HGHandle handle, Map<String, Json> frozenProperties)
    {
        this.node = node;
        this.handle = handle;
        this.properties = Collections.unmodifiableMap(withHandleProperty(node, handle, frozenProperties));
    }

    /**
     * An entity loaded from storage. Takes ownership of <code>frozenProperties</code>. The atom type
     * doesn't know the atom's handle, so <code>handle</code> may be <code>null</code>; it is then bound
     * by {@link HyperNodeJson#get(HGHandle)} with {@link #bindHandle(HGHandle)}.
     */
    static HGObjectJson loaded(HyperNodeJson node, HGHandle handle, Map<String, Json> frozenProperties)
    {
        return new HGObjectJson(node, handle, frozenProperties);
    }

    /** Entity mode: set the handle of an entity loaded without one. */
    synchronized void bindHandle(HGHandle handle)
    {
        if (this.handle != null)
            return;
        this.handle = handle;
        properties = Collections.unmodifiableMap(withHandleProperty(node, handle, new HashMap<String, Json>(properties)));
    }

    private static Map<String, Json> withHandleProperty(HyperNodeJson node, HGHandle handle, Map<String, Json> m)
    {
        String handleProperty = node.getEntityInterface().entityHandleProperty();
        if (handleProperty != null && handle != null)
            m.put(handleProperty, Json.make(handle.getPersistent().toString()));
        return m;
    }

    /** Whether this object is the canonical runtime object of a stored entity. */
    public boolean isEntityObject()
    {
        return node != null;
    }

    /** The handle of the entity, or <code>null</code> in plain mode. */
    public HGHandle getEntityHandle()
    {
        return handle;
    }

    HyperNodeJson node()
    {
        return node;
    }

    /**
     * Switch from plain to entity mode. Called by {@link HyperNodeJson} inside the transaction that
     * adds the entity; if that transaction aborts, the object goes back to plain mode with its
     * original properties.
     */
    synchronized void promote(HyperNodeJson node, HGHandle handle, Map<String, Json> frozenProperties)
    {
        final Map<String, Json> plainProperties = properties;
        HGTransaction tx = node.graph().getTransactionManager().getContext().getCurrent();
        if (tx != null)
        {
            final HGTransaction top = tx.getTopLevel();
            // This action undoes everything done to the object in this transaction, so later changes
            // in the same transaction must not register their own restore action (abort actions run
            // in no particular order).
            restoreRegisteredFor = top;
            top.addAbortAction(new Runnable() {
                public void run()
                {
                    synchronized (HGObjectJson.this)
                    {
                        HGObjectJson.this.node = null;
                        HGObjectJson.this.handle = null;
                        HGObjectJson.this.properties = plainProperties;
                        if (restoreRegisteredFor == top)
                            restoreRegisteredFor = null;
                    }
                }
            });
            top.addCommitAction(new Runnable() {
                public void run()
                {
                    synchronized (HGObjectJson.this)
                    {
                        if (restoreRegisteredFor == top)
                            restoreRegisteredFor = null;
                    }
                }
            });
        }
        this.handle = handle;
        this.properties = Collections.unmodifiableMap(withHandleProperty(node, handle, new HashMap<String, Json>(frozenProperties)));
        this.node = node;
    }

    /** Entity mode: replace all properties (except the handle property) with frozen ones. */
    synchronized void replaceState(Map<String, Json> frozenProperties)
    {
        beforeChange();
        properties = Collections.unmodifiableMap(withHandleProperty(node, handle, new HashMap<String, Json>(frozenProperties)));
    }

    /** Entity mode: the current immutable snapshot of the properties. */
    Map<String, Json> snapshot()
    {
        return properties;
    }

    /** Entity mode: set a property to an already frozen value. The caller holds the lock. */
    void putFrozen(String name, Json frozenValue)
    {
        beforeChange();
        Map<String, Json> m = new HashMap<String, Json>(properties);
        m.put(name, frozenValue);
        properties = Collections.unmodifiableMap(m);
    }

    // On the first change inside a top-level transaction, remember the current snapshot and
    // restore it if the transaction aborts. The caller holds the lock.
    private void beforeChange()
    {
        HGTransaction tx = node.graph().getTransactionManager().getContext().getCurrent();
        if (tx == null)
            return;
        final HGTransaction top = tx.getTopLevel();
        if (restoreRegisteredFor == top)
            return;
        restoreRegisteredFor = top;
        final Map<String, Json> saved = properties;
        top.addAbortAction(new Runnable() {
            public void run()
            {
                synchronized (HGObjectJson.this)
                {
                    properties = saved;
                    if (restoreRegisteredFor == top)
                        restoreRegisteredFor = null;
                }
            }
        });
        top.addCommitAction(new Runnable() {
            public void run()
            {
                synchronized (HGObjectJson.this)
                {
                    if (restoreRegisteredFor == top)
                        restoreRegisteredFor = null;
                }
            }
        });
    }

    // ------------------------------------------------------------------
    // Reading
    // ------------------------------------------------------------------

    public boolean isObject() { return true; }

    /**
     * In entity mode, nested objects and arrays are returned as access paths from this entity, so
     * that changing them changes this entity (by copy-on-write). Primitives and entity references
     * are returned as they are.
     */
    public Json at(String property)
    {
        Json value = properties.get(property);
        if (node != null && value != null && (value.isObject() || value.isArray()))
            return new AccessPathJson(this, property);
        return value;
    }

    public boolean has(String property) { return properties.containsKey(property); }

    public boolean is(String property, Object value)
    {
        Json p = properties.get(property);
        return p != null && p.equals(make(value));
    }

    /** In entity mode, an unmodifiable snapshot of the properties, with frozen values. */
    public Map<String, Json> asJsonMap() { return properties; }

    public Map<String, Object> asMap()
    {
        Map<String, Object> m = new HashMap<String, Object>();
        for (Map.Entry<String, Json> e : properties.entrySet())
            m.put(e.getKey(), e.getValue().getValue());
        return m;
    }

    public Object getValue() { return asMap(); }

    public Iterator<Json> iterator() { return properties.values().iterator(); }

    /** A plain, mutable deep copy (including the handle property of an entity). */
    public Json dup()
    {
        HGObjectJson copy = new HGObjectJson();
        for (Map.Entry<String, Json> e : properties.entrySet())
            copy.properties.put(e.getKey(), e.getValue().dup());
        return copy;
    }

    // ------------------------------------------------------------------
    // Changing
    // ------------------------------------------------------------------

    public Json set(String property, Json value)
    {
        if (property == null)
            throw new IllegalArgumentException("Null property names are not allowed, value is " + value);
        if (value == null)
            value = Json.nil();
        if (node == null)
        {
            properties.put(property, value);
            return this;
        }
        // Freeze outside the lock: freezing may add nested entities to the database.
        Json frozen = node.freeze(value, 0);
        synchronized (this)
        {
            putFrozen(property, frozen);
        }
        return this;
    }

    public Json atDel(String property)
    {
        if (node == null)
            return properties.remove(property);
        synchronized (this)
        {
            Json removed = properties.get(property);
            if (removed != null)
            {
                beforeChange();
                Map<String, Json> m = new HashMap<String, Json>(properties);
                m.remove(property);
                properties = Collections.unmodifiableMap(m);
            }
            return removed;
        }
    }

    public Json delAt(String property)
    {
        atDel(property);
        return this;
    }

    public Json with(Json other, Json... options)
    {
        if (other == null)
            return this;
        if (!other.isObject())
            throw new UnsupportedOperationException();
        if (node == null)
        {
            if (options.length == 0)
                properties.putAll(other.asJsonMap());
            else
                mergeIntoPlain(other, options);
            return this;
        }
        // Work on a mutable copy with mjson's own logic, then freeze the result (outside the lock,
        // since freezing may add nested entities to the database).
        Json copy = dup();
        copy.with(other, options);
        Map<String, Json> m = new HashMap<String, Json>();
        for (Map.Entry<String, Json> e : copy.asJsonMap().entrySet())
            m.put(e.getKey(), node.freeze(e.getValue(), 0));
        synchronized (this)
        {
            beforeChange();
            properties = Collections.unmodifiableMap(m);
        }
        return this;
    }

    private void mergeIntoPlain(Json other, Json... options)
    {
        Json mjsonObject = Json.defaultFactory.object();
        mjsonObject.asJsonMap().putAll(properties);
        mjsonObject.with(other, options);
        properties.clear();
        properties.putAll(mjsonObject.asJsonMap());
    }

    protected Json withOptions(Json other, Json allOptions, String path)
    {
        // Merging a property value into this object (with "merge"): the options for this object
        // apply, which only matter for nested elements; at this level the properties are merged.
        return with(other);
    }

    // ------------------------------------------------------------------

    public String toString() { return toString(Integer.MAX_VALUE); }
    public String toString(int maxCharacters) { return JsonText.object(properties, maxCharacters); }
    public int hashCode() { return properties.hashCode(); }
    public boolean equals(Object x)
    {
        return x instanceof Json && ((Json)x).isObject() && properties.equals(((Json)x).asJsonMap());
    }
}
