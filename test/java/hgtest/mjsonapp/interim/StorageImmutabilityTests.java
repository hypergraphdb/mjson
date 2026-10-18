package hgtest.mjsonapp.interim;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.hypergraphdb.HGPersistentHandle;
import org.junit.Test;

import mjson.Json;
import mjson.hgdb.JsonNodeException;
import mjson.hgdb.JsonTypeSchema;

/**
 * Group S of docs/test-plan-interim.md: storage never modifies a value atom.
 */
public class StorageImmutabilityTests extends JsonModelTestBase
{
    static final String ASSIA_VALUE =
        "{\"name\":\"Assia\",\"age\":50,\"address\":{\"street\":\"4078 Berri\",\"city\":\"Montreal\"}}";
    static final String BORIS_VALUE =
        "{\"name\":\"Boris\",\"age\":50,\"address\":{\"street\":\"4078 Berri\",\"city\":\"Montreal\"}}";

    /** S1: the notebook scenario with non-entities. Replacing a value is rejected and nothing changes. */
    @Test
    public void s1_nonEntitySharingNestedValueCannotBeReplaced()
    {
        final HGPersistentHandle ah = add(ASSIA_VALUE);
        final HGPersistentHandle bh = add(BORIS_VALUE);
        assertEquals("precondition: address shared", valueOf(ah, "address"), valueOf(bh, "address"));
        final Json modified = Json.read(ASSIA_VALUE.replace("4078", "4076"));
        expectException(() -> node.replace(ah, modified, JsonTypeSchema.objectTypeHandle), JsonNodeException.class);
        bothViews(() -> {
            assertEquals("4078 Berri", str(ah, "address", "street"));
            assertEquals("4078 Berri", str(bh, "address", "street"));
        });
    }

    /** S2: editing a shared nested value of entity A leaves B and the original value atom untouched. */
    @Test
    public void s2_entityNestedEditDoesNotLeakToOtherEntity()
    {
        final Shared f = sharedFixture();
        Json a = get(f.a);
        a.at("address").set("street", "S2");
        node.update(a);
        bothViews(() -> {
            assertEquals("S2", str(f.a, "address", "street"));
            assertEquals("other entity must not change", "S1", str(f.b, "address", "street"));
            assertEquals("original shared value atom must not change", "S1", str(f.address, "street"));
            assertDifferent("A must point to a new address value", f.address, valueOf(f.a, "address"));
            assertEquals("B still points to the original value", f.address, valueOf(f.b, "address"));
        });
    }

    /** S3: bug 1 in EXPERIMENT-NOTES.md, appending to a shared array (non-empty: see X5a). */
    @Test
    public void s3_appendToSharedArrayDoesNotLeak()
    {
        final Shared f = sharedFixture();
        Json a = get(f.a);
        a.at("tags").add(Json.make("x"));
        node.update(a);
        bothViews(() -> {
            assertEquals(Json.read("[\"t\",\"x\"]"), at(get(f.a), "tags"));
            assertEquals("other entity's array must not change", Json.read("[\"t\"]"), at(get(f.b), "tags"));
            assertEquals("the shared array atom must not change", Json.read("[\"t\"]"), get(f.tags));
            assertEquals(f.tags, valueOf(f.b, "tags"));
        });
    }

    /** S4: an updated nested value equal to an existing value deduplicates to that value. */
    @Test
    public void s4_updatedNestedValueDeduplicatesToExistingValue()
    {
        final Shared f = sharedFixture();
        final HGPersistentHandle existing = add("{\"street\":\"S3\",\"city\":\"C\"}");
        Json a = get(f.a);
        a.at("address").set("street", "S3");
        node.update(a);
        bothViews(() -> {
            assertEquals(existing, valueOf(f.a, "address"));
            assertEquals("S1", str(f.b, "address", "street"));
        });
    }

    /** S5: changing a nested value and changing it back points to the original shared atom again. */
    @Test
    public void s5_changeAndRevertReturnsToOriginalValue()
    {
        final Shared f = sharedFixture();
        Json a = get(f.a);
        a.at("address").set("street", "S2");
        node.update(a);
        a = get(f.a);
        a.at("address").set("street", "S1");
        node.update(a);
        bothViews(() -> {
            assertEquals(f.address, valueOf(f.a, "address"));
            assertEquals("S1", str(f.address, "street"));
        });
    }

    /** S6: a nested object inside an array inside an entity. */
    @Test
    public void s6_objectInsideArrayInsideEntity()
    {
        String order = "{\"entity\":\"order\",\"no\":\"%s\",\"lines\":[{\"sku\":\"a\",\"qty\":1},{\"sku\":\"b\",\"qty\":2}]}";
        final HGPersistentHandle o1 = add(String.format(order, "1"));
        final HGPersistentHandle o2 = add(String.format(order, "2"));
        final HGPersistentHandle lines = valueOf(o1, "lines");
        assertEquals("precondition: lines shared", lines, valueOf(o2, "lines"));
        Json o = get(o1);
        o.at("lines").at(0).set("qty", 5);
        node.update(o);
        bothViews(() -> {
            assertEquals(5, num(o1, "lines", 0, "qty"));
            assertEquals("other order must not change", 1, num(o2, "lines", 0, "qty"));
            assertEquals("original lines atom must not change", 1, at(get(lines), 0, "qty").asInteger());
            assertEquals(lines, valueOf(o2, "lines"));
        });
    }

    /** S7: replacing with a state that drops and adds properties; dropped values are not collected (D7). */
    @Test
    public void s7_dropAndAddProperties()
    {
        final Shared f = sharedFixture();
        Json a = get(f.a);
        a.delAt("tags");
        a.set("nick", "As");
        node.update(a);
        bothViews(() -> {
            Json stored = get(f.a);
            assertFalse(stored.has("tags"));
            assertEquals("As", stored.at("nick").asString());
            assertNotNull("dropped value atom still exists", get(f.tags));
            assertEquals(Json.read("[\"t\"]"), get(f.tags));
            assertEquals(Json.read("[\"t\"]"), at(get(f.b), "tags"));
        });
    }
}
