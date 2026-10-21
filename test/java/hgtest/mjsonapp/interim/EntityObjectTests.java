package hgtest.mjsonapp.interim;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.hypergraphdb.HGPersistentHandle;
import org.junit.Test;

import mjson.Json;

/**
 * Group E of docs/test-plan-interim.md: the canonical entity object.
 */
public class EntityObjectTests extends JsonModelTestBase
{
    static final String COMPANY = "{\"entity\":\"company\",\"name\":\"Acme\"}";

    /** E1: get() returns the one canonical object for an entity. */
    @Test
    public void e1_getReturnsCanonicalObject()
    {
        HGPersistentHandle h = add(PERSON_A);
        assertSame(get(h), get(h));
    }

    /** E2: mutate a top-level property and update; same canonical object afterwards. */
    @Test
    public void e2_topLevelMutationAndUpdate()
    {
        final HGPersistentHandle h = add(PERSON_A);
        Json a = get(h);
        a.set("name", "Assia2");
        node.update(a);
        assertSame(a, get(h));
        bothViews(() -> assertEquals("Assia2", str(h, "name")));
    }

    /** E3: update() of a detached copy identified only by its hghandle property. */
    @Test
    public void e3_updateByHandleProperty()
    {
        final HGPersistentHandle h = add(PERSON_A);
        Json copy = get(h).dup();
        assertTrue("precondition: copy carries hghandle", copy.has("hghandle"));
        copy.set("name", "FromCopy");
        node.update(copy);
        bothViews(() -> assertEquals("FromCopy", str(h, "name")));
    }

    /** E4a: updating from an external object via add() keeps the canonical object and doesn't retain the external one. */
    @Test
    public void e4a_externalUpdateViaAddKeepsCanonicalObject()
    {
        final HGPersistentHandle h = add(PERSON_A);
        Json canonical = get(h);
        Json external = Json.read(PERSON_A.replace("Assia", "Ext")).set("hghandle", h.toString());
        node.add(external);
        assertSame("canonical object must stay the same", canonical, get(h));
        assertEquals("Ext", canonical.at("name").asString());
        external.set("name", "MUT");
        bothViews(() -> assertEquals("Ext", str(h, "name")));
    }

    /** E4b: same, via update(). */
    @Test
    public void e4b_externalUpdateViaUpdateKeepsCanonicalObject()
    {
        final HGPersistentHandle h = add(PERSON_A);
        Json canonical = get(h);
        Json external = Json.read(PERSON_A.replace("Assia", "Ext")).set("hghandle", h.toString());
        node.update(external);
        assertSame("canonical object must stay the same", canonical, get(h));
        assertEquals("Ext", canonical.at("name").asString());
        external.set("name", "MUT");
        bothViews(() -> assertEquals("Ext", str(h, "name")));
    }

    /** E5: a nested entity is seen as a reference from its parent, in both views. */
    @Test
    public void e5_nestedEntityIsAReference()
    {
        final HGPersistentHandle ch = add(COMPANY);
        Json person = Json.object("entity", "person", "name", "E", "employer", get(ch));
        final HGPersistentHandle ph = add(person);
        bothViews(() -> {
            Json employer = get(ph).at("employer");
            assertTrue("employer must be a reference string, got " + employer, employer.isString());
            assertEquals(ch, refToHandle(employer));
        });
    }

    /** E6: updating the nested entity doesn't change the parent's stored state; resolving the reference shows the change. */
    @Test
    public void e6_updatingNestedEntity()
    {
        final HGPersistentHandle ch = add(COMPANY);
        final HGPersistentHandle ph = add(Json.object("entity", "person", "name", "E", "employer", get(ch)));
        final HGPersistentHandle employerProperty = propertyOf(ph, "employer");
        Json company = get(ch);
        company.set("name", "Acme2");
        node.update(company);
        bothViews(() -> {
            assertEquals("parent's stored state unchanged", employerProperty, propertyOf(ph, "employer"));
            assertEquals(ch, valueOf(ph, "employer"));
            assertEquals("Acme2", str(refToHandle(get(ph).at("employer")), "name"));
        });
    }

    /** E7: iterating an entity while another thread mutates it never fails and sees complete states. */
    @Test
    public void e7_snapshotConsistentIteration() throws Exception
    {
        HGPersistentHandle h = add(PERSON_A);
        final Json a = get(h);
        final AtomicBoolean done = new AtomicBoolean(false);
        final int writes = 2000;
        runConcurrently(2, thread -> {
            if (thread == 0)
            {
                for (int i = 0; i < writes; i++)
                    a.set("k" + i, i);
                done.set(true);
            }
            else
            {
                while (!done.get())
                {
                    Map<String, Json> copy = new HashMap<String, Json>();
                    for (Map.Entry<String, Json> e : a.asJsonMap().entrySet())
                        copy.put(e.getKey(), e.getValue());
                    assertTrue("snapshot must contain the original properties", copy.containsKey("name"));
                }
            }
        });
        assertEquals(writes - 1, a.at("k" + (writes - 1)).asInteger());
    }
}
