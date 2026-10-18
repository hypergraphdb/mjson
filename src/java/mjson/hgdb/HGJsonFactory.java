package mjson.hgdb;

import java.util.Map;

import mjson.Json;

/**
 * <p>
 * The <code>Json.Factory</code> an application storing JSON through {@link HyperNodeJson} must use
 * for everything it persists: install it with <code>Json.setGlobalFactory</code>, or per thread
 * with <code>Json.attachFactory</code>. JSON objects are created as {@link HGObjectJson}, which
 * <code>HyperNodeJson.add</code> can make the canonical object of an entity. Arrays and primitives
 * are mjson's own. JSON created elsewhere is converted with {@link HyperNodeJson#adopt(Json)}.
 * </p>
 */
public class HGJsonFactory implements Json.Factory
{
    public Json nil() { return Json.defaultFactory.nil(); }
    public Json bool(boolean value) { return Json.defaultFactory.bool(value); }
    public Json string(String value) { return Json.defaultFactory.string(value); }
    public Json number(Number value) { return Json.defaultFactory.number(value); }
    public Json array() { return Json.defaultFactory.array(); }
    public Json object() { return new HGObjectJson(); }

    public Json make(Object anything)
    {
        // mjson's default factory creates map-based objects with its own object(), not the active factory's.
        if (anything instanceof Map<?, ?>)
        {
            Json O = object();
            for (Map.Entry<?, ?> e : ((Map<?, ?>)anything).entrySet())
                O.set(e.getKey().toString(), Json.factory().make(e.getValue()));
            return O;
        }
        return Json.defaultFactory.make(anything);
    }
}
