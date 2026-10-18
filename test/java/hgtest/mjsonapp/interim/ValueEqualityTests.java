package hgtest.mjsonapp.interim;

import static org.junit.Assert.assertEquals;

import org.hypergraphdb.HGPersistentHandle;
import org.junit.Test;

import mjson.Json;

/**
 * Group X of docs/test-plan-interim.md: value equality as used by deduplication. Tests marked LIMIT
 * pin current behavior that the final version is expected to change (exact numeric equality).
 */
public class ValueEqualityTests extends JsonModelTestBase
{
    /** X1 LIMIT: 50 and 50.0 are the same atom and read back as 50.0. */
    @Test
    public void x1_limit_integerAndDoubleShareAtom()
    {
        final HGPersistentHandle h1 = add(Json.make(50));
        HGPersistentHandle h2 = add(Json.make(50.0));
        assertEquals(h1, h2);
        reopen();
        assertEquals("50.0", get(h1).toString());
    }

    /** X2 LIMIT: integers that differ only above 2^53 collide. */
    @Test
    public void x2_limit_largeLongsCollide()
    {
        long big = 1L << 53;
        HGPersistentHandle h1 = add(Json.make(big));
        HGPersistentHandle h2 = add(Json.make(big + 1));
        assertEquals(h1, h2);
    }

    /** X3: object properties are unordered. */
    @Test
    public void x3_objectPropertyOrderIrrelevant()
    {
        HGPersistentHandle h1 = add("{\"a\":\"1\",\"b\":\"2\",\"c\":\"3\"}");
        HGPersistentHandle h2 = add("{\"c\":\"3\",\"a\":\"1\",\"b\":\"2\"}");
        assertEquals(h1, h2);
    }

    /** X4: array element order matters. */
    @Test
    public void x4_arrayOrderMatters()
    {
        HGPersistentHandle h1 = add("[\"1\",\"2\"]");
        HGPersistentHandle h2 = add("[\"2\",\"1\"]");
        assertDifferent("arrays in different order must be different atoms", h1, h2);
    }

    /** X5a: the empty array is one atom, distinct from non-empty arrays added before it. */
    @Test
    public void x5a_emptyArray()
    {
        HGPersistentHandle nonEmpty = add("[\"1\",\"2\"]");
        HGPersistentHandle e1 = add("[]");
        HGPersistentHandle e2 = add("[]");
        assertEquals(e1, e2);
        assertDifferent("[] must not match a non-empty array", nonEmpty, e1);
        assertEquals(Json.array(), get(e1));
    }

    /** X5b: the empty object is one atom, distinct from non-empty objects added before it. */
    @Test
    public void x5b_emptyObject()
    {
        HGPersistentHandle nonEmpty = add("{\"a\":\"1\"}");
        HGPersistentHandle e1 = add("{}");
        HGPersistentHandle e2 = add("{}");
        assertEquals(e1, e2);
        assertDifferent("{} must not match a non-empty object", nonEmpty, e1);
        assertEquals(Json.object(), get(e1));
    }

    /** X5c: the empty array and the empty object are different atoms. */
    @Test
    public void x5c_emptyArrayIsNotEmptyObject()
    {
        assertDifferent("[] and {} must be different atoms", add("[]"), add("{}"));
    }

    /** X6: a null-valued property is not the same as a missing property (pins current behavior). */
    @Test
    public void x6_nullPropertyVersusMissing()
    {
        HGPersistentHandle withNull = add("{\"a\":null,\"b\":\"1\"}");
        HGPersistentHandle without = add("{\"b\":\"1\"}");
        assertDifferent("{a:null,b} and {b} must be different atoms", withNull, without);
    }
}
