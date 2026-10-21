package hgtest.mjsonapp.interim;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.junit.Test;

import mjson.Json;
import mjson.hgdb.JsonNodeException;
import mjson.hgdb.JsonTypeSchema;

/**
 * Group R of docs/test-plan-interim.md: only entities can be replaced or updated.
 */
public class ReplaceRulesTests extends JsonModelTestBase
{
    static final String VALUE = "{\"street\":\"S9\",\"city\":\"C\"}";

    /** R1a: replace on an object value handle is rejected. */
    @Test
    public void r1a_replaceObjectValueRejected()
    {
        final HGPersistentHandle vh = add(VALUE);
        expectException(() -> node.replace(vh, Json.read("{\"street\":\"S10\",\"city\":\"C\"}"),
                                           JsonTypeSchema.objectTypeHandle),
                        JsonNodeException.class);
        bothViews(() -> assertEquals("S9", str(vh, "street")));
    }

    /** R1b: replace on an array value handle is rejected. */
    @Test
    public void r1b_replaceArrayValueRejected()
    {
        final HGPersistentHandle vh = add("[\"x\",\"y\"]");
        expectException(() -> node.replace(vh, Json.read("[\"x\",\"y\",\"z\"]"), JsonTypeSchema.arrayTypeHandle),
                        JsonNodeException.class);
        bothViews(() -> assertEquals(Json.read("[\"x\",\"y\"]"), get(vh)));
    }

    /** R1c: update() of a value obtained from get() is rejected. */
    @Test
    public void r1c_updateOfValueRejected()
    {
        final HGPersistentHandle vh = add(VALUE);
        final Json v = get(vh);
        expectException(() -> node.update(v), JsonNodeException.class);
        bothViews(() -> assertEquals("S9", str(vh, "street")));
    }

    /** R2: replacing a primitive stays refused (existing behavior). */
    @Test
    public void r2_replacePrimitiveRejected()
    {
        final HGPersistentHandle h = add("\"hello\"");
        expectException(() -> node.replace(h, Json.make("bye"), JsonTypeSchema.stringTypeHandle),
                        JsonNodeException.class, IllegalArgumentException.class);
        bothViews(() -> assertEquals("hello", get(h).asString()));
    }

    /** R3a: an entity cannot be replaced by a non-entity (D3). */
    @Test
    public void r3a_entityToNonEntityRejected()
    {
        final HGPersistentHandle h = add(PERSON_A);
        expectException(() -> node.replace(h, Json.read("{\"name\":\"NotAnEntity\"}"), JsonTypeSchema.objectTypeHandle),
                        JsonNodeException.class);
        bothViews(() -> assertEquals("Assia", str(h, "name")));
    }

    /** R3b: a value cannot be turned into an entity by replacing it (D3). */
    @Test
    public void r3b_valueToEntityRejected()
    {
        final HGPersistentHandle vh = add(VALUE);
        expectException(() -> node.replace(vh, Json.read("{\"entity\":\"place\",\"street\":\"S9\",\"city\":\"C\"}"),
                                           JsonTypeSchema.objectTypeHandle),
                        JsonNodeException.class);
        bothViews(() -> assertTrue("still a value", !get(vh).has("entity")));
    }

    /** R4: non-JSON atoms are still replaced through HyperGraph. */
    @Test
    public void r4_nonJsonReplaceDelegates()
    {
        final HGPersistentHandle h = graph.add("plain").getPersistent();
        HGHandle stringType = graph.getTypeSystem().getTypeHandle(String.class);
        assertTrue(node.replace(h, "other", stringType));
        bothViews(() -> assertEquals("other", graph.get(h)));
    }

    /** R5: define() of a non-JSON instance works (no fall-through to a cast to Json). */
    @Test
    public void r5_defineNonJson()
    {
        final HGPersistentHandle h = graph.getHandleFactory().makeHandle();
        node.define(h, graph.getTypeSystem().getTypeHandle(String.class), "hello", 0);
        bothViews(() -> assertEquals("hello", graph.get(h)));
    }

    /** R6: add() of a request-body-like object carrying the hghandle of an existing entity updates it. */
    @Test
    public void r6_addWithExistingHandleUpdatesEntity()
    {
        final Shared f = sharedFixture();
        Json body = Json.read(PERSON_A.replace("S1", "S2")).set("hghandle", f.a.toString());
        HGHandle h = node.add(body);
        assertEquals(f.a, h.getPersistent());
        bothViews(() -> {
            assertEquals("S2", str(f.a, "address", "street"));
            assertEquals("S1", str(f.b, "address", "street"));
            assertEquals("S1", str(f.address, "street"));
        });
    }
}
