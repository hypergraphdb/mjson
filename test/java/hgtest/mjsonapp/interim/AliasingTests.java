package hgtest.mjsonapp.interim;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.junit.Test;

import mjson.Json;

/**
 * Group A of docs/test-plan-interim.md: the library never keeps or mutates the caller's objects
 * in a way that lets the caller change stored or cached state.
 */
public class AliasingTests extends JsonModelTestBase
{
    static final String ADDRESS = "{\"street\":\"S1\",\"city\":\"C\"}";

    /** A1: add() of a value doesn't keep the caller's instance. */
    @Test
    public void a1_addedValueIsNotTheCallersObject()
    {
        Json v = Json.read(ADDRESS);
        final HGPersistentHandle h = add(v);
        v.set("street", "MUT");
        assertNotSame(v, get(h));
        bothViews(() -> assertEquals("S1", str(h, "street")));
    }

    /** A2: a nested value of an added entity is not the caller's nested object. */
    @Test
    public void a2_entityNestedValueIsNotTheCallersObject()
    {
        Json addr = Json.read(ADDRESS);
        Json e = Json.object("entity", "person", "name", "E", "address", addr);
        final HGPersistentHandle h = add(e);
        final HGPersistentHandle addrHandle = valueOf(h, "address");
        addr.set("street", "MUT");
        bothViews(() -> {
            assertEquals("S1", str(h, "address", "street"));
            assertEquals("S1", str(addrHandle, "street"));
        });
    }

    /** A3: entity.set(name, plainJson) stores a copy, so later changes to plainJson don't leak in. */
    @Test
    public void a3_setOnEntityCopiesTheValue()
    {
        final HGPersistentHandle h = add(PERSON_A);
        Json a = get(h);
        Json plain = Json.read("{\"street\":\"P\",\"city\":\"C\"}");
        a.set("address", plain);
        plain.set("street", "MUT");
        assertEquals("P", at(a, "address", "street").asString());
        node.update(a);
        bothViews(() -> assertEquals("P", str(h, "address", "street")));
    }

    /** A4a: add() of a new entity with a preassigned hghandle doesn't remove it from the caller's object. */
    @Test
    public void a4a_addWithNewHandleDoesNotMutateInput()
    {
        HGPersistentHandle preassigned = graph.getHandleFactory().makeHandle();
        Json body = Json.read(PERSON_A).set("hghandle", preassigned.toString());
        Json before = body.dup();
        HGHandle h = node.add(body);
        assertEquals(preassigned, h.getPersistent());
        assertEquals("caller's object must not be modified", before, body);
    }

    /** A4b: add() acting as an update of an existing entity doesn't modify the caller's object. */
    @Test
    public void a4b_addAsUpdateDoesNotMutateInput()
    {
        HGPersistentHandle h = add(PERSON_A);
        Json body = Json.read(PERSON_A.replace("S1", "S2")).set("hghandle", h.toString());
        Json before = body.dup();
        node.add(body);
        assertEquals("caller's object must not be modified", before, body);
    }

    /**
     * A5: get() doesn't write into the entity on every call. Probe: remove hghandle from the canonical
     * object, call get() again, and check it wasn't put back.
     */
    @Test
    public void a5_getDoesNotWriteIntoEntity()
    {
        HGPersistentHandle h = add(PERSON_A);
        Json a = get(h);
        assertTrue("precondition: hghandle set when loaded/added", a.has("hghandle"));
        a.delAt("hghandle");
        for (int i = 0; i < 3; i++)
            get(h);
        assertFalse("get() must not write hghandle into the entity again", a.has("hghandle"));
    }
}
