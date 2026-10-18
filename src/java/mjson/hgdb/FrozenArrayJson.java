package mjson.hgdb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import mjson.Json;

/**
 * <p>
 * An immutable JSON array: a value in the sense of {@link HyperNodeJson}. Its elements are frozen
 * values, immutable primitives or entity references. Every mutator throws
 * {@link ImmutableJsonException}; use {@link #dup()} to get a mutable copy.
 * </p>
 */
public final class FrozenArrayJson extends Json
{
    private static final long serialVersionUID = 1L;

    private final List<Json> elements;

    /** Takes ownership of <code>elements</code>, which must already be frozen. */
    FrozenArrayJson(List<Json> elements)
    {
        this.elements = Collections.unmodifiableList(elements);
    }

    private ImmutableJsonException immutable()
    {
        return new ImmutableJsonException("Attempt to modify an immutable JSON value; use dup() to get a mutable copy.", this);
    }

    public boolean isArray() { return true; }
    public Json at(int index) { return elements.get(index); }
    public boolean is(int index, Object value)
    {
        return index >= 0 && index < elements.size() && elements.get(index).equals(make(value));
    }
    public List<Json> asJsonList() { return elements; }
    public List<Object> asList()
    {
        List<Object> L = new ArrayList<Object>();
        for (Json x : elements)
            L.add(x.getValue());
        return L;
    }
    public Object getValue() { return asList(); }
    public Iterator<Json> iterator() { return elements.iterator(); }

    /** A plain, mutable deep copy. */
    public Json dup()
    {
        Json copy = Json.array();
        for (Json x : elements)
            copy.add(x.dup());
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
    public String toString(int maxCharacters) { return JsonText.array(elements, maxCharacters); }
    public int hashCode() { return elements.hashCode(); }
    public boolean equals(Object x)
    {
        return x instanceof Json && ((Json)x).isArray() && elements.equals(((Json)x).asJsonList());
    }
}
