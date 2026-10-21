package mjson.hgdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import mjson.Json;

/**
 * <p>
 * A nested object or array of an entity, reached from the entity with <code>at(...)</code>. A new
 * access path is created for every dereference; it holds the root entity and the path from it (property
 * names and array indices), and reads always resolve the path against the entity's current state.
 * </p>
 * <p>
 * Changing an access path changes the entity only, by copy-on-write: new frozen nodes are built from
 * the changed one up to the entity, sharing every untouched subtree, and the entity's top-level property
 * is replaced. The values that were there before are not changed, nor is any other entity or value that
 * shares them.
 * </p>
 * <p>
 * An array index in a path refers to the array the path went through when it was created. If that array
 * has been replaced since (for example an element was removed through another path), writing through
 * the path throws a {@link JsonNodeException} instead of changing a different element. Access in a single
 * expression, like <code>entity.at("items").at(3).set("n", 1)</code>, never trips that check.
 * </p>
 */
public final class AccessPathJson extends Json
{
    private static final long serialVersionUID = 1L;

    private final HGObjectJson root;
    // path[0] is a property of the root; the others are property names (String) or indices (Integer)
    private final Object[] path;
    // indexedArrays[k] is the array the index path[k] was resolved against, or null for a name
    private final Json[] indexedArrays;

    AccessPathJson(HGObjectJson root, String property)
    {
        this(root, new Object[] { property }, new Json[] { null });
    }

    private AccessPathJson(HGObjectJson root, Object[] path, Json[] indexedArrays)
    {
        this.root = root;
        this.path = path;
        this.indexedArrays = indexedArrays;
    }

    private String describe()
    {
        StringBuilder sb = new StringBuilder();
        for (Object step : path)
            sb.append('/').append(step);
        return sb.toString();
    }

    private static Json step(Json container, Object step)
    {
        if (container == null)
            return null;
        if (step instanceof Integer)
        {
            int i = (Integer)step;
            return container.isArray() && i >= 0 && i < container.asJsonList().size() ? container.at(i) : null;
        }
        return container.isObject() ? container.asJsonMap().get((String)step) : null;
    }

    /** The current (frozen) value at this path. */
    Json resolve()
    {
        Json current = root.snapshot().get((String)path[0]);
        for (int k = 1; k < path.length && current != null; k++)
            current = step(current, path[k]);
        if (current == null)
            throw new JsonNodeException("The access path " + describe() + " no longer leads to a value of the entity.", root);
        return current;
    }

    private Json child(Object step)
    {
        Json container = resolve();
        Json value = step(container, step);
        if (value != null && (value.isObject() || value.isArray()))
        {
            Object[] p = Arrays.copyOf(path, path.length + 1);
            p[path.length] = step;
            Json[] a = Arrays.copyOf(indexedArrays, indexedArrays.length + 1);
            a[indexedArrays.length] = step instanceof Integer ? container : null;
            return new AccessPathJson(root, p, a);
        }
        return value;
    }

    // ------------------------------------------------------------------
    // Copy-on-write
    // ------------------------------------------------------------------

    private interface Change
    {
        Json apply(Json current);
    }

    private Json write(Change change)
    {
        synchronized (root)
        {
            if (!root.isEntityObject())
                throw new JsonNodeException("The access path " + describe() + " belongs to an object that is no longer an entity.", root);
            Json[] chain = new Json[path.length];
            Json current = root.snapshot().get((String)path[0]);
            chain[0] = current;
            for (int k = 1; k < path.length; k++)
            {
                if (path[k] instanceof Integer && current != indexedArrays[k])
                    throw new JsonNodeException("Stale access path " + describe() + ": the array it indexes has changed since the path was created.", root);
                current = step(current, path[k]);
                chain[k] = current;
            }
            if (current == null)
                throw new JsonNodeException("The access path " + describe() + " no longer leads to a value of the entity.", root);
            Json replacement = change.apply(current);
            for (int k = path.length - 1; k >= 1; k--)
                replacement = replaced(chain[k - 1], path[k], replacement);
            root.putFrozen((String)path[0], replacement);
            return replacement;
        }
    }

    private static Json replaced(Json container, Object step, Json child)
    {
        if (step instanceof Integer)
        {
            List<Json> L = new ArrayList<Json>(container.asJsonList());
            L.set((Integer)step, child);
            return new FrozenArrayJson(L);
        }
        Map<String, Json> m = new HashMap<String, Json>(container.asJsonMap());
        m.put((String)step, child);
        return new FrozenObjectJson(m);
    }

    private Json freeze(Json value)
    {
        return root.node().freeze(value, 1);
    }

    private static UnsupportedOperationException notAn(String kind)
    {
        return new UnsupportedOperationException("The value at this access path is not an " + kind + ".");
    }

    // ------------------------------------------------------------------
    // Reading
    // ------------------------------------------------------------------

    public boolean isObject() { return resolve().isObject(); }
    public boolean isArray() { return resolve().isArray(); }
    public Json at(String property) { return child(property); }
    public Json at(int index) { return child(index); }
    public boolean has(String property) { return resolve().has(property); }
    public boolean is(String property, Object value) { return resolve().is(property, value); }
    public boolean is(int index, Object value) { return resolve().is(index, value); }
    public Map<String, Json> asJsonMap() { return resolve().asJsonMap(); }
    public List<Json> asJsonList() { return resolve().asJsonList(); }
    public Map<String, Object> asMap() { return resolve().asMap(); }
    public List<Object> asList() { return resolve().asList(); }
    public Object getValue() { return resolve().getValue(); }
    public Iterator<Json> iterator() { return resolve().iterator(); }
    public Json dup() { return resolve().dup(); }

    // ------------------------------------------------------------------
    // Changing (each returns this path, like mjson's own setters)
    // ------------------------------------------------------------------

    public Json set(final String property, final Json value)
    {
        if (property == null)
            throw new IllegalArgumentException("Null property names are not allowed, value is " + value);
        final Json frozen = freeze(value == null ? Json.nil() : value);
        write(new Change() { public Json apply(Json current) {
            if (!current.isObject())
                throw notAn("object");
            Map<String, Json> m = new HashMap<String, Json>(current.asJsonMap());
            m.put(property, frozen);
            return new FrozenObjectJson(m);
        }});
        return this;
    }

    public Json atDel(final String property)
    {
        final Json[] removed = new Json[1];
        write(new Change() { public Json apply(Json current) {
            if (!current.isObject())
                throw notAn("object");
            Map<String, Json> m = new HashMap<String, Json>(current.asJsonMap());
            removed[0] = m.remove(property);
            return new FrozenObjectJson(m);
        }});
        return removed[0];
    }

    public Json delAt(String property)
    {
        atDel(property);
        return this;
    }

    public Json add(Json element)
    {
        final Json frozen = freeze(element == null ? Json.nil() : element);
        write(new Change() { public Json apply(Json current) {
            if (!current.isArray())
                throw notAn("array");
            List<Json> L = new ArrayList<Json>(current.asJsonList());
            L.add(frozen);
            return new FrozenArrayJson(L);
        }});
        return this;
    }

    public Json set(final int index, Object value)
    {
        final Json frozen = freeze(make(value));
        write(new Change() { public Json apply(Json current) {
            if (!current.isArray())
                throw notAn("array");
            List<Json> L = new ArrayList<Json>(current.asJsonList());
            L.set(index, frozen);
            return new FrozenArrayJson(L);
        }});
        return this;
    }

    public Json atDel(final int index)
    {
        final Json[] removed = new Json[1];
        write(new Change() { public Json apply(Json current) {
            if (!current.isArray())
                throw notAn("array");
            List<Json> L = new ArrayList<Json>(current.asJsonList());
            removed[0] = L.remove(index);
            return new FrozenArrayJson(L);
        }});
        return removed[0];
    }

    public Json delAt(int index)
    {
        atDel(index);
        return this;
    }

    public Json remove(final Json element)
    {
        write(new Change() { public Json apply(Json current) {
            if (!current.isArray())
                throw notAn("array");
            List<Json> L = new ArrayList<Json>(current.asJsonList());
            L.remove(element);
            return new FrozenArrayJson(L);
        }});
        return this;
    }

    public Json with(final Json other, final Json... options)
    {
        write(new Change() { public Json apply(Json current) {
            // Use mjson's own logic on a mutable copy, then freeze the result.
            Json copy = current.dup();
            copy.with(other, options);
            return freeze(copy);
        }});
        return this;
    }

    protected Json withOptions(Json other, Json allOptions, String path)
    {
        return with(other);
    }

    // ------------------------------------------------------------------

    public String toString() { return resolve().toString(); }
    public String toString(int maxCharacters) { return resolve().toString(maxCharacters); }
    public int hashCode() { return resolve().hashCode(); }
    public boolean equals(Object x)
    {
        return resolve().equals(x instanceof AccessPathJson ? ((AccessPathJson)x).resolve() : x);
    }
}
