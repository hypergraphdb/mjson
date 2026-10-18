package hgtest.mjsonapp.interim;

import static org.junit.Assert.assertEquals;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.hypergraphdb.HGPersistentHandle;
import org.junit.Test;

import mjson.Json;
import mjson.hgdb.ImmutableJsonException;

/**
 * Group V of docs/test-plan-interim.md: values are frozen in memory.
 */
public class FrozenValueTests extends JsonModelTestBase
{
    static final String VALUE =
        "{\"street\":\"S1\",\"city\":\"C\",\"nested\":{\"k\":\"v\"},\"list\":[\"x\",\"y\"]}";
    static final String ARRAY = "[\"x\",\"y\",{\"a\":\"b\"}]";

    /** V1: every object mutator throws ImmutableJsonException and leaves the value unchanged. */
    @Test
    public void v1_objectMutatorsThrow()
    {
        final HGPersistentHandle vh = add(VALUE);
        final Json v = get(vh);
        Map<String, ThrowingRunnable> m = new LinkedHashMap<String, ThrowingRunnable>();
        m.put("set", () -> v.set("street", "X"));
        m.put("atDel", () -> v.atDel("city"));
        m.put("delAt", () -> v.delAt("city"));
        m.put("with", () -> v.with(Json.object("extra", "1")));
        assertAllThrow(ImmutableJsonException.class, m);
        bothViews(() -> assertEquals(Json.read(VALUE), get(vh)));
    }

    /** V2: every array mutator throws ImmutableJsonException and leaves the value unchanged. */
    @Test
    public void v2_arrayMutatorsThrow()
    {
        final HGPersistentHandle vh = add(ARRAY);
        final Json v = get(vh);
        Map<String, ThrowingRunnable> m = new LinkedHashMap<String, ThrowingRunnable>();
        m.put("add", () -> v.add(Json.make("z")));
        m.put("remove", () -> v.remove(Json.make("x")));
        m.put("set(int)", () -> v.set(0, "q"));
        m.put("atDel(int)", () -> v.atDel(0));
        m.put("delAt(int)", () -> v.delAt(0));
        assertAllThrow(ImmutableJsonException.class, m);
        bothViews(() -> assertEquals(Json.read(ARRAY), get(vh)));
    }

    /** V3: the collection views and iterators can't be used to mutate a value. */
    @Test
    public void v3_collectionViewsAreUnmodifiable()
    {
        final HGPersistentHandle oh = add(VALUE);
        final HGPersistentHandle ah = add(ARRAY);
        final Json o = get(oh);
        final Json a = get(ah);
        Map<String, ThrowingRunnable> m = new LinkedHashMap<String, ThrowingRunnable>();
        m.put("asJsonMap().put", () -> o.asJsonMap().put("z", Json.make("z")));
        m.put("asJsonMap().remove", () -> o.asJsonMap().remove("street"));
        m.put("asJsonMap().entrySet().iterator().remove", () -> {
            Iterator<Map.Entry<String, Json>> it = o.asJsonMap().entrySet().iterator();
            it.next();
            it.remove();
        });
        m.put("asJsonMap().values().iterator().remove", () -> {
            Iterator<Json> it = o.asJsonMap().values().iterator();
            it.next();
            it.remove();
        });
        m.put("asJsonList().add", () -> a.asJsonList().add(Json.make("z")));
        m.put("asJsonList().set", () -> a.asJsonList().set(0, Json.make("z")));
        m.put("asJsonList().iterator().remove", () -> {
            Iterator<Json> it = a.asJsonList().iterator();
            it.next();
            it.remove();
        });
        assertAllThrow(UnsupportedOperationException.class, m);
        bothViews(() -> {
            assertEquals(Json.read(VALUE), get(oh));
            assertEquals(Json.read(ARRAY), get(ah));
        });
    }

    /** V4: nested nodes of a frozen value are frozen too. */
    @Test
    public void v4_nestedNodesAreFrozen()
    {
        final HGPersistentHandle vh = add(VALUE);
        final Json v = get(vh);
        Map<String, ThrowingRunnable> m = new LinkedHashMap<String, ThrowingRunnable>();
        m.put("nested.set", () -> v.at("nested").set("k", "w"));
        m.put("list.add", () -> v.at("list").add(Json.make("z")));
        assertAllThrow(ImmutableJsonException.class, m);
        bothViews(() -> assertEquals(Json.read(VALUE), get(vh)));
    }

    /** V5: dup() gives a plain mutable deep copy, independent of the stored value. */
    @Test
    public void v5_dupIsMutableAndIndependent()
    {
        final HGPersistentHandle vh = add(VALUE);
        Json copy = get(vh).dup();
        copy.set("street", "S2");
        copy.at("nested").set("k", "w");
        copy.at("list").add(Json.make("z"));
        assertEquals("S2", copy.at("street").asString());
        bothViews(() -> assertEquals(Json.read(VALUE), get(vh)));
    }

    /**
     * V6: placing a frozen value inside a user-built object is allowed and doesn't change the value.
     * The parent (Json.up()) of a shared value is meaningless and not tested: a shared value has many
     * parents, which mjson's single-parent design can't represent.
     */
    @Test
    public void v6_placingFrozenValueInAnotherObject()
    {
        final HGPersistentHandle vh = add(VALUE);
        Json v = get(vh);
        Json holder = Json.object();
        holder.set("a", v);
        assertEquals(v, holder.at("a"));
        bothViews(() -> assertEquals(Json.read(VALUE), get(vh)));
    }

    /** V7: value equality between frozen values and equal plain Json. */
    @Test
    public void v7_equalityWithPlainJson()
    {
        HGPersistentHandle vh = add(VALUE);
        Json frozen = get(vh);
        Json plain = Json.read(VALUE);
        assertEquals(plain, frozen);
        assertEquals(frozen, plain);
        assertEquals(plain.hashCode(), frozen.hashCode());
    }

    /** V8: values are frozen after being reloaded from storage as well. */
    @Test
    public void v8_frozenAfterReload()
    {
        final HGPersistentHandle vh = add(VALUE);
        reopen();
        final Json v = get(vh);
        Map<String, ThrowingRunnable> m = new LinkedHashMap<String, ThrowingRunnable>();
        m.put("set", () -> v.set("street", "X"));
        m.put("nested.set", () -> v.at("nested").set("k", "w"));
        m.put("list.add", () -> v.at("list").add(Json.make("z")));
        assertAllThrow(ImmutableJsonException.class, m);
        assertEquals(Json.read(VALUE), get(vh));
    }
}
