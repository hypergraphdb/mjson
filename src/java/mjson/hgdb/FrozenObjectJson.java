package mjson.hgdb;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import mjson.Json;

/**
 * <p>
 * An immutable JSON object: a value in the sense of {@link HyperNodeJson}, that is a JSON object
 * that is not an entity. Its properties are frozen values, immutable primitives or entity
 * references. Every mutator throws {@link ImmutableJsonException}; use {@link #dup()} to get a
 * mutable copy.
 * </p>
 */
public final class FrozenObjectJson extends Json
{
    private static final long serialVersionUID = 1L;

    static final FrozenObjectJson EMPTY = new FrozenObjectJson(new HashMap<String, Json>());

    private final Map<String, Json> properties;

    /** Takes ownership of <code>properties</code>, whose values must already be frozen. */
    FrozenObjectJson(Map<String, Json> properties)
    {
        this.properties = Collections.unmodifiableMap(properties);
    }

    private ImmutableJsonException immutable()
    {
        return new ImmutableJsonException("Attempt to modify an immutable JSON value; use dup() to get a mutable copy.", this);
    }

    public boolean isObject() { return true; }
    public Json at(String property) { return properties.get(property); }
    public boolean has(String property) { return properties.containsKey(property); }
    public boolean is(String property, Object value)
    {
        Json p = properties.get(property);
        return p != null && p.equals(make(value));
    }
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

    /** A plain, mutable deep copy. */
    public Json dup()
    {
        Json copy = Json.object();
        for (Map.Entry<String, Json> e : properties.entrySet())
            copy.set(e.getKey(), e.getValue().dup());
        return copy;
    }

    public Json set(String property, Json value) { throw immutable(); }
    public Json set(int index, Object value) { throw immutable(); }
    public Json add(Json element) { throw immutable(); }
    public Json remove(Json element) { throw immutable(); }
    public Json atDel(String property) { throw immutable(); }
    public Json atDel(int index) { throw immutable(); }
    public Json delAt(String property) { throw immutable(); }
    public Json delAt(int index) { throw immutable(); }
    public Json with(Json other, Json... options) { throw immutable(); }
    public Json with(Json other, Object... options) { throw immutable(); }
    protected Json withOptions(Json other, Json allOptions, String path) { throw immutable(); }

    public String toString() { return toString(Integer.MAX_VALUE); }
    public String toString(int maxCharacters) { return JsonText.object(properties, maxCharacters); }
    public int hashCode() { return properties.hashCode(); }
    public boolean equals(Object x)
    {
        return x instanceof Json && ((Json)x).isObject() && properties.equals(((Json)x).asJsonMap());
    }
}
