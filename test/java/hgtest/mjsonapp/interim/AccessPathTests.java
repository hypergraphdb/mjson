package hgtest.mjsonapp.interim;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import org.hypergraphdb.HGPersistentHandle;
import org.junit.Test;

import mjson.Json;
import mjson.hgdb.ImmutableJsonException;
import mjson.hgdb.JsonNodeException;

/**
 * Group P of docs/test-plan-interim.md: access paths from an entity and copy-on-write.
 */
public class AccessPathTests extends JsonModelTestBase
{
    static final String ITEMS =
        "{\"entity\":\"list\",\"name\":\"%s\",\"items\":[{\"n\":1},{\"n\":2},{\"n\":3}],\"address\":{\"street\":\"S1\",\"city\":\"C\"}}";

    /** P1: editing through a path changes the entity only; shared values and other entities are untouched. */
    @Test
    public void p1_pathEditWithoutUpdate()
    {
        Shared f = sharedFixture();
        Json a = get(f.a);
        a.at("address").set("street", "X");
        assertEquals("X", at(a, "address", "street").asString());
        assertEquals("S1", str(f.address, "street"));
        assertEquals("S1", str(f.b, "address", "street"));
    }

    /** P2: path edit followed by update is persisted for that entity only. */
    @Test
    public void p2_pathEditThenUpdate()
    {
        final Shared f = sharedFixture();
        Json a = get(f.a);
        a.at("address").set("street", "X");
        node.update(a);
        bothViews(() -> {
            assertEquals("X", str(f.a, "address", "street"));
            assertEquals("S1", str(f.b, "address", "street"));
        });
    }

    /**
     * P3: deep copy-on-write with structural sharing. asJsonMap() of an entity exposes the stored
     * (frozen) property values, which lets the test check object identity of untouched subtrees.
     */
    @Test
    public void p3_deepPathStructuralSharing()
    {
        HGPersistentHandle h = add("{\"entity\":\"t\",\"a\":{\"b\":{\"c\":{\"v\":1}},\"sib\":{\"s\":1}},\"other\":{\"o\":1}}");
        Json e = get(h);
        Json aBefore = e.asJsonMap().get("a");
        Json sibBefore = aBefore.asJsonMap().get("sib");
        Json otherBefore = e.asJsonMap().get("other");

        e.at("a").at("b").at("c").set("v", 2);

        assertEquals(2, at(e, "a", "b", "c", "v").asInteger());
        Json aAfter = e.asJsonMap().get("a");
        assertNotSame("nodes on the path are new", aBefore, aAfter);
        assertEquals("the original node is unchanged", 1, at(aBefore, "b", "c", "v").asInteger());
        assertSame("untouched sibling subtree is shared", sibBefore, aAfter.asJsonMap().get("sib"));
        assertSame("untouched top-level property is shared", otherBefore, e.asJsonMap().get("other"));
    }

    /** P4: two paths obtained separately see each other's writes. */
    @Test
    public void p4_pathsSeeEachOthersWrites()
    {
        HGPersistentHandle h = add(PERSON_A);
        Json a = get(h);
        Json p1 = a.at("address");
        Json p2 = a.at("address");
        p1.set("street", "X1");
        assertEquals("X1", p2.at("street").asString());
        p2.set("city", "Y");
        assertEquals("Y", p1.at("city").asString());
        assertEquals("X1", p1.at("street").asString());
        assertEquals("X1", at(a, "address", "street").asString());
        assertEquals("Y", at(a, "address", "city").asString());
    }

    /** P5: editing an array element through a path changes only that element of that entity. */
    @Test
    public void p5_arrayElementThroughPath()
    {
        HGPersistentHandle h1 = add(String.format(ITEMS, "one"));
        HGPersistentHandle h2 = add(String.format(ITEMS, "two"));
        HGPersistentHandle items = valueOf(h1, "items");
        assertEquals("precondition: items shared", items, valueOf(h2, "items"));
        Json e = get(h1);
        e.at("items").at(1).set("n", 20);
        assertEquals(Json.read("[{\"n\":1},{\"n\":20},{\"n\":3}]"), at(e, "items"));
        assertEquals(Json.read("[{\"n\":1},{\"n\":2},{\"n\":3}]"), at(get(h2), "items"));
        assertEquals(Json.read("[{\"n\":1},{\"n\":2},{\"n\":3}]"), get(items));
    }

    /**
     * P6a: a path through an array that changed since the path was created is detected on write
     * (stale-path check, see the plan, pitfall P5) instead of silently editing another element.
     */
    @Test
    public void p6a_stalePathDetectedOnWrite()
    {
        HGPersistentHandle h = add(String.format(ITEMS, "one"));
        Json e = get(h);
        final Json p = e.at("items").at(1);
        e.at("items").delAt(0);
        expectException(() -> p.set("n", 99), JsonNodeException.class);
        assertEquals(Json.read("[{\"n\":2},{\"n\":3}]"), at(e, "items"));
    }

    /** P6b: single-expression access after a structural change works. */
    @Test
    public void p6b_singleExpressionAfterStructuralChange()
    {
        HGPersistentHandle h = add(String.format(ITEMS, "one"));
        Json e = get(h);
        e.at("items").delAt(0);
        e.at("items").at(0).set("n", 7);
        assertEquals(Json.read("[{\"n\":7},{\"n\":3}]"), at(e, "items"));
    }

    /** P6c: a path made only of property names survives changes elsewhere in the entity. */
    @Test
    public void p6c_namePathSurvivesOtherChanges()
    {
        HGPersistentHandle h = add(String.format(ITEMS, "one"));
        Json e = get(h);
        Json p = e.at("address");
        e.at("items").delAt(0);
        p.set("street", "Z");
        assertEquals("Z", at(e, "address", "street").asString());
        assertEquals(Json.read("[{\"n\":2},{\"n\":3}]"), at(e, "items"));
    }

    /** P7: paths stop at entity references; the referenced entity can't be mutated through its parent. */
    @Test
    public void p7_pathStopsAtEntityReference()
    {
        final HGPersistentHandle ch = add("{\"entity\":\"company\",\"name\":\"Acme\"}");
        HGPersistentHandle ph = add(Json.object("entity", "person", "name", "E", "employer", get(ch)));
        final Json person = get(ph);
        expectException(() -> person.at("employer").set("name", "Hacked"), RuntimeException.class);
        bothViews(() -> assertEquals("Acme", str(ch, "name")));
    }

    /** P8: a path whose intermediate property was removed fails clearly on write. */
    @Test
    public void p8_pathToRemovedProperty()
    {
        HGPersistentHandle h = add(PERSON_A);
        Json a = get(h);
        final Json p = a.at("address");
        a.delAt("address");
        expectException(() -> p.set("street", "Z"), JsonNodeException.class);
        assertFalse(a.has("address"));
    }

    /** P9: a value fetched directly by its handle (not through an entity) can't be mutated. */
    @Test
    public void p9_valueFetchedDirectlyIsFrozen()
    {
        final Shared f = sharedFixture();
        final Json addr = get(f.address);
        expectException(() -> addr.set("street", "Z"), ImmutableJsonException.class);
        bothViews(() -> {
            assertEquals("S1", str(f.address, "street"));
            assertEquals("S1", str(f.a, "address", "street"));
        });
    }
}
