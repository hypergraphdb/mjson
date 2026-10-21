package hgtest.mjsonapp.interim;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.hypergraphdb.HGPersistentHandle;
import org.junit.Test;

import mjson.Json;

/**
 * Group C of docs/test-plan-interim.md. Checks the interim guarantees only: no corruption and no
 * leaks between entities. Isolation between threads mutating the same entity is not promised in
 * the interim version (pitfalls P1/P2); C8 reports on it without failing.
 *
 * Iterations and thread counts: -Dinterim.iterations=N, -Dinterim.threads=N.
 */
public class ConcurrencyTests extends JsonModelTestBase
{
    /** Create one entity per thread, all sharing the same address and tags values. */
    private HGPersistentHandle[] entitiesSharingValues(int n)
    {
        HGPersistentHandle[] E = new HGPersistentHandle[n];
        for (int i = 0; i < n; i++)
            E[i] = add(PERSON_A.replace("Assia", "P" + i));
        for (int i = 1; i < n; i++)
            assertEquals("precondition: address shared", valueOf(E[0], "address"), valueOf(E[i], "address"));
        return E;
    }

    /** C1: each thread edits only its own entity, through paths; nothing leaks between entities. */
    @Test
    public void c1_ownEntitiesNoCrossLeaks() throws Exception
    {
        final int n = THREADS;
        final HGPersistentHandle[] E = entitiesSharingValues(n);
        final HGPersistentHandle sharedAddress = valueOf(E[0], "address");
        final HGPersistentHandle sharedTags = valueOf(E[0], "tags");
        final AtomicInteger attempts = new AtomicInteger();
        runConcurrently(n, t -> {
            for (int i = 0; i < ITERATIONS; i++)
            {
                final int iteration = i;
                tx(() -> {
                    attempts.incrementAndGet();
                    Json e = get(E[t]);
                    e.at("address").set("street", "T" + t + "-" + iteration);
                    e.at("tags").add(Json.make(iteration));
                    node.update(e);
                    return null;
                });
                // Read every entity and check that it only ever shows its own data.
                for (int j = 0; j < n; j++)
                {
                    String street = at(get(E[j]), "address", "street").asString();
                    assertTrue("entity " + j + " shows foreign street " + street,
                               street.equals("S1") || street.startsWith("T" + j + "-"));
                }
            }
        });
        System.out.println("c1: " + attempts.get() + " transaction attempts for " + n * ITERATIONS + " commits");
        bothViews(() -> {
            Json expectedTags = Json.read("[\"t\"]");
            for (int i = 0; i < ITERATIONS; i++)
                expectedTags.add(Json.make(i));
            for (int t = 0; t < n; t++)
            {
                assertEquals("T" + t + "-" + (ITERATIONS - 1), str(E[t], "address", "street"));
                assertEquals("entity " + t + " tags", expectedTags, at(get(E[t]), "tags"));
            }
            assertEquals("S1", str(sharedAddress, "street"));
            assertEquals(Json.read("[\"t\"]"), get(sharedTags));
            checkIntegrity();
        });
    }

    /** C2a: concurrent adds of the same new value produce a single atom. */
    @Test
    public void c2a_concurrentIdenticalValuesDeduplicate() throws Exception
    {
        final int n = THREADS;
        final Set<HGPersistentHandle> handles = java.util.Collections.synchronizedSet(new HashSet<HGPersistentHandle>());
        runConcurrently(n, t -> {
            for (int i = 0; i < ITERATIONS; i++)
                handles.add(tx(() -> add("{\"k\":\"shared\",\"n\":[\"1\",\"2\"]}")));
        });
        assertEquals("one atom for the value: " + handles, 1, handles.size());
        checkIntegrity();
    }

    /** C2b: concurrent adds of entities containing the same new nested value share one value atom. */
    @Test
    public void c2b_concurrentEntitiesShareNewNestedValue() throws Exception
    {
        final int n = THREADS;
        final List<HGPersistentHandle> entities = java.util.Collections.synchronizedList(new ArrayList<HGPersistentHandle>());
        runConcurrently(n, t -> {
            for (int i = 0; i < ITERATIONS; i++)
            {
                final int iteration = i;
                entities.add(tx(() -> add("{\"entity\":\"e\",\"id\":\"" + t + "-" + iteration
                                          + "\",\"nested\":{\"k\":\"fresh\",\"list\":[\"a\"]}}")));
            }
        });
        Set<HGPersistentHandle> nested = new HashSet<HGPersistentHandle>();
        for (HGPersistentHandle e : entities)
            nested.add(valueOf(e, "nested"));
        assertEquals("one atom for the nested value", 1, nested.size());
        checkIntegrity();
    }

    /** C3: concurrent writers adding entities of the same type: all committed and readable. */
    @Test
    public void c3_concurrentAddsOfSameType() throws Exception
    {
        final int n = THREADS;
        final List<HGPersistentHandle> handles = java.util.Collections.synchronizedList(new ArrayList<HGPersistentHandle>());
        final AtomicInteger attempts = new AtomicInteger();
        runConcurrently(n, t -> {
            for (int i = 0; i < ITERATIONS; i++)
            {
                final String id = t + "-" + i;
                handles.add(tx(() -> {
                    attempts.incrementAndGet();
                    return add("{\"entity\":\"widget\",\"id\":\"" + id + "\"}");
                }));
            }
        });
        System.out.println("c3: " + attempts.get() + " transaction attempts for " + n * ITERATIONS + " commits");
        assertEquals(n * ITERATIONS, new HashSet<HGPersistentHandle>(handles).size());
        reopen();
        Set<String> ids = new HashSet<String>();
        for (HGPersistentHandle h : handles)
        {
            Json w = get(h);
            assertNotNull("entity " + h + " not found after reopen", w);
            ids.add(w.at("id").asString());
        }
        assertEquals(n * ITERATIONS, ids.size());
        checkIntegrity();
    }

    /** C4: readers of shared values never see them change while writers update entities that use them. */
    @Test
    public void c4_readersNeverSeeSharedValuesChange() throws Exception
    {
        final int writers = Math.max(1, THREADS / 2);
        final HGPersistentHandle[] E = entitiesSharingValues(writers);
        final HGPersistentHandle sharedAddress = valueOf(E[0], "address");
        final Json expected = Json.read("{\"street\":\"S1\",\"city\":\"C\"}");
        final AtomicBoolean writing = new AtomicBoolean(true);
        final AtomicInteger finishedWriters = new AtomicInteger();
        runConcurrently(writers * 2, t -> {
            if (t < writers)
            {
                for (int i = 0; i < ITERATIONS; i++)
                {
                    final int iteration = i;
                    tx(() -> {
                        Json e = get(E[t]);
                        e.at("address").set("street", "W" + t + "-" + iteration);
                        node.update(e);
                        return null;
                    });
                }
                if (finishedWriters.incrementAndGet() == writers)
                    writing.set(false);
            }
            else
            {
                while (writing.get())
                {
                    assertEquals("shared value changed", expected, get(sharedAddress));
                    node.getAll(org.hypergraphdb.HGQuery.hg.type(mjson.hgdb.JsonTypeSchema.objectTypeHandle));
                }
            }
        });
        bothViews(() -> {
            assertEquals(expected, get(sharedAddress));
            checkIntegrity();
        });
    }

    /** C5: bug 3 in the notes. Readers iterate an entity while a writer mutates and updates it. */
    @Test
    public void c5_noConcurrentModificationWhileIterating() throws Exception
    {
        final HGPersistentHandle h = add(PERSON_A);
        final AtomicBoolean writing = new AtomicBoolean(true);
        runConcurrently(3, t -> {
            if (t == 0)
            {
                try
                {
                    for (int i = 0; i < ITERATIONS * 10; i++)
                    {
                        final int iteration = i;
                        tx(() -> {
                            Json e = get(h);
                            e.set("k" + iteration, iteration);
                            node.update(e);
                            return null;
                        });
                    }
                }
                finally
                {
                    writing.set(false);
                }
            }
            else
            {
                while (writing.get())
                {
                    Json e = get(h);
                    int count = 0;
                    for (Map.Entry<String, Json> entry : e.asJsonMap().entrySet())
                        if (entry.getValue() != null)
                            count++;
                    assertTrue(count > 0);
                    assertNotNull(e.toString());
                }
            }
        });
        bothViews(() -> assertEquals(ITERATIONS * 10 - 1, num(h, "k" + (ITERATIONS * 10 - 1))));
    }

    /** C6: threads edit different entities that share a deep nested value. */
    @Test
    public void c6_deepSharedValueUnderConcurrentEdits() throws Exception
    {
        final int n = THREADS;
        final String template = "{\"entity\":\"user\",\"id\":\"%d\",\"profile\":{\"prefs\":{\"theme\":\"dark\"},\"langs\":[\"en\"]}}";
        final HGPersistentHandle[] E = new HGPersistentHandle[n];
        for (int i = 0; i < n; i++)
            E[i] = add(String.format(template, i));
        final HGPersistentHandle sharedProfile = valueOf(E[0], "profile");
        runConcurrently(n, t -> {
            for (int i = 0; i < ITERATIONS; i++)
            {
                final int iteration = i;
                tx(() -> {
                    Json e = get(E[t]);
                    e.at("profile").at("prefs").set("theme", "t" + t + "-" + iteration);
                    node.update(e);
                    return null;
                });
            }
        });
        bothViews(() -> {
            for (int t = 0; t < n; t++)
            {
                assertEquals("t" + t + "-" + (ITERATIONS - 1), str(E[t], "profile", "prefs", "theme"));
                assertEquals(Json.read("[\"en\"]"), at(get(E[t]), "profile", "langs"));
            }
            assertEquals(Json.read("{\"prefs\":{\"theme\":\"dark\"},\"langs\":[\"en\"]}"), get(sharedProfile));
            checkIntegrity();
        });
    }

    /** C7: integrity sweep after a mixed workload of adds, updates and removals of entities. */
    @Test
    public void c7_integrityAfterMixedWorkload() throws Exception
    {
        final int n = THREADS;
        runConcurrently(n, t -> {
            List<HGPersistentHandle> mine = new ArrayList<HGPersistentHandle>();
            for (int i = 0; i < ITERATIONS; i++)
            {
                final int iteration = i;
                mine.add(tx(() -> add(PERSON_A.replace("Assia", "M" + t + "-" + iteration))));
                final HGPersistentHandle target = mine.get(mine.size() / 2);
                tx(() -> {
                    Json e = get(target);
                    e.at("address").set("city", "City" + iteration);
                    e.at("tags").add(Json.make("t" + iteration));
                    node.update(e);
                    return null;
                });
                if (i % 5 == 4)
                {
                    final HGPersistentHandle victim = mine.remove(0);
                    tx(() -> node.remove(victim));
                }
            }
        });
        checkIntegrity();
        reopen();
        checkIntegrity();
    }

    /**
     * C8 LIMIT (pitfall P1): two threads set different properties of the same entity and update it.
     * Lost writes are possible in the interim version; this test reports what happened and does not
     * fail on it.
     */
    @Test
    public void c8_limit_lostWritesOnSameEntity() throws Exception
    {
        final HGPersistentHandle h = add(PERSON_A);
        runConcurrently(2, t -> {
            for (int i = 0; i < ITERATIONS; i++)
            {
                final int iteration = i;
                tx(() -> {
                    Json e = get(h);
                    e.set(t == 0 ? "left" : "right", iteration);
                    node.update(e);
                    return null;
                });
            }
        });
        reopen();
        Json e = get(h);
        boolean bothPresent = e.has("left") && e.has("right")
            && e.at("left").asInteger() == ITERATIONS - 1 && e.at("right").asInteger() == ITERATIONS - 1;
        System.out.println("c8 (LIMIT, informational): final state " + e
                           + (bothPresent ? " - no lost write observed" : " - lost write observed"));
        if (!e.has("name"))
            fail("entity lost its original properties: " + e);
    }
}
