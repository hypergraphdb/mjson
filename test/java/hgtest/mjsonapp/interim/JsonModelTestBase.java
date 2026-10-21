package hgtest.mjsonapp.interim;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HGValueLink;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.util.HGUtils;
import org.junit.After;
import org.junit.Before;

import mjson.Json;
import mjson.hgdb.HyperNodeJson;
import mjson.hgdb.JsonProperty;
import mjson.hgdb.JsonTypeSchema;

/**
 * Base class for the interim storage model tests (docs/test-plan-interim.md).
 *
 * Every test gets a fresh database. Results are usually checked in two views:
 * the cache view (same session) and the storage view (after reopening the graph,
 * so atoms are rebuilt from storage). See {@link #bothViews(Runnable)}.
 *
 * All handles kept by tests are persistent handles, so they stay valid across reopens.
 */
public abstract class JsonModelTestBase
{
    /** Iterations per thread in concurrency tests; raise with -Dinterim.iterations=N for stress runs. */
    protected static final int ITERATIONS = Integer.getInteger("interim.iterations", 20);
    protected static final int THREADS = Integer.getInteger("interim.threads", 4);
    protected static final long DIAGNOSTIC_INTERVAL_MINUTES = Long.getLong("interim.diagnosticMinutes", 10);

    protected static final String PERSON_A =
        "{\"entity\":\"person\",\"name\":\"Assia\",\"address\":{\"street\":\"S1\",\"city\":\"C\"},\"tags\":[\"t\"]}";
    protected static final String PERSON_B =
        "{\"entity\":\"person\",\"name\":\"Boris\",\"address\":{\"street\":\"S1\",\"city\":\"C\"},\"tags\":[\"t\"]}";

    protected File location;
    protected HyperGraph graph;
    protected HyperNodeJson node;

    @Before
    public void openFreshDatabase() throws IOException
    {
        // Applications must create the JSON they store with the HyperNodeJson factory (D8).
        Json.setGlobalFactory(HyperNodeJson.factory());
        location = Files.createTempDirectory("hgjson-interim").toFile();
        open();
    }

    @After
    public void closeAndDropDatabase()
    {
        try
        {
            if (graph != null && graph.isOpen())
                graph.close();
        }
        finally
        {
            HGUtils.dropHyperGraphInstance(location.getAbsolutePath());
            Json.setGlobalFactory(Json.defaultFactory);
        }
    }

    protected void open()
    {
        HGConfiguration config = new HGConfiguration();
        config.getTypeConfiguration().addSchema(new JsonTypeSchema());
        graph = HGEnvironment.get(location.getAbsolutePath(), config);
        node = new HyperNodeJson(graph);
    }

    /** Close and reopen the graph: subsequent reads come from storage, not from objects cached earlier. */
    protected void reopen()
    {
        graph.close();
        open();
    }

    /** Run the checks against the cache view, then reopen and run them against the storage view. */
    protected void bothViews(Runnable checks)
    {
        try
        {
            checks.run();
        }
        catch (AssertionError e)
        {
            throw new AssertionError("[cache view] " + e.getMessage(), e);
        }
        reopen();
        try
        {
            checks.run();
        }
        catch (AssertionError e)
        {
            throw new AssertionError("[storage view] " + e.getMessage(), e);
        }
    }

    // ------------------------------------------------------------------
    // Data access helpers
    // ------------------------------------------------------------------

    protected HGPersistentHandle add(String json)
    {
        return node.add(Json.read(json)).getPersistent();
    }

    protected HGPersistentHandle add(Json json)
    {
        return node.add(json).getPersistent();
    }

    protected Json get(HGHandle h)
    {
        Json j = node.get(h);
        return j;
    }

    /** Navigate a path of property names (String) and array indices (Integer). */
    protected static Json at(Json j, Object... path)
    {
        Json current = j;
        StringBuilder walked = new StringBuilder();
        for (Object step : path)
        {
            walked.append('/').append(step);
            if (current == null)
                fail("Nothing at path " + walked);
            current = step instanceof Integer ? current.at((Integer)step) : current.at((String)step);
        }
        assertNotNull("Nothing at path " + walked, current);
        return current;
    }

    protected String str(HGHandle h, Object... path)
    {
        return at(get(h), path).asString();
    }

    protected int num(HGHandle h, Object... path)
    {
        return at(get(h), path).asInteger();
    }

    /** The JsonProperty link of the stored object <code>owner</code> for property <code>name</code>. */
    protected HGPersistentHandle propertyOf(HGHandle owner, String name)
    {
        HGValueLink link = graph.get(owner);
        assertNotNull("No atom for " + owner, link);
        for (HGHandle propHandle : link)
        {
            JsonProperty prop = graph.get(propHandle);
            String propName = graph.get(prop.getName());
            if (name.equals(propName))
                return propHandle.getPersistent();
        }
        fail("Stored object " + owner + " has no property " + name);
        return null;
    }

    /** The value atom that the stored object <code>owner</code> links to for property <code>name</code>. */
    protected HGPersistentHandle valueOf(HGHandle owner, String name)
    {
        JsonProperty prop = graph.get(propertyOf(owner, name));
        return prop.getValue().getPersistent();
    }

    protected HGPersistentHandle refToHandle(Json ref)
    {
        HGHandle h = ref == null ? null : node.getEntityInterface().entityReferenceToHandle(node, ref);
        if (h == null)
            fail("Expected an entity reference, got: " + ref);
        return h.getPersistent();
    }

    protected <T> T tx(Callable<T> work)
    {
        return graph.getTransactionManager().transact(work);
    }

    // ------------------------------------------------------------------
    // Shared-value fixture
    // ------------------------------------------------------------------

    /**
     * Two entities A and B created separately, sharing their address value and their tags array.
     * Note: the tags array is non-empty on purpose; empty arrays are currently not deduplicated (test X5a).
     */
    protected static class Shared
    {
        public HGPersistentHandle a, b, address, tags, addressProperty;
    }

    protected Shared sharedFixture()
    {
        Shared f = new Shared();
        f.a = add(PERSON_A);
        f.b = add(PERSON_B);
        f.address = valueOf(f.a, "address");
        f.tags = valueOf(f.a, "tags");
        f.addressProperty = propertyOf(f.a, "address");
        assertEquals("fixture: address value shared", f.address, valueOf(f.b, "address"));
        assertEquals("fixture: tags value shared", f.tags, valueOf(f.b, "tags"));
        assertEquals("fixture: address property link shared", f.addressProperty, propertyOf(f.b, "address"));
        return f;
    }

    // ------------------------------------------------------------------
    // Assertions
    // ------------------------------------------------------------------

    public interface ThrowingRunnable
    {
        void run() throws Exception;
    }

    /** Assert that <code>action</code> throws an exception of one of the given types; returns it. */
    @SafeVarargs
    protected static Throwable expectException(ThrowingRunnable action, Class<? extends Throwable>... expected)
    {
        try
        {
            action.run();
        }
        catch (Throwable t)
        {
            for (Class<? extends Throwable> c : expected)
                if (c.isInstance(t))
                    return t;
            AssertionError e = new AssertionError("Expected one of " + names(expected) + " but got " + t);
            e.initCause(t);
            throw e;
        }
        fail("Expected one of " + names(expected) + " but nothing was thrown");
        return null;
    }

    /** Run every action and fail listing those that did not throw the expected exception type. */
    protected static void assertAllThrow(Class<? extends Throwable> expected, Map<String, ThrowingRunnable> actions)
    {
        List<String> failures = new ArrayList<String>();
        for (Map.Entry<String, ThrowingRunnable> e : actions.entrySet())
        {
            try
            {
                e.getValue().run();
                failures.add(e.getKey() + ": nothing thrown");
            }
            catch (Throwable t)
            {
                if (!expected.isInstance(t))
                    failures.add(e.getKey() + ": threw " + t);
            }
        }
        if (!failures.isEmpty())
            fail("Expected " + expected.getSimpleName() + " from every operation; failures: " + failures);
    }

    protected static void assertDifferent(String message, Object unexpected, Object actual)
    {
        if (unexpected == null ? actual == null : unexpected.equals(actual))
            fail(message + " (both are " + actual + ")");
    }

    private static String names(Class<?>[] classes)
    {
        List<String> L = new ArrayList<String>();
        for (Class<?> c : classes)
            L.add(c.getSimpleName());
        return L.toString();
    }

    // ------------------------------------------------------------------
    // Concurrency
    // ------------------------------------------------------------------

    public interface ThreadBody
    {
        void run(int thread) throws Exception;
    }

    /**
     * Start <code>threads</code> threads behind a barrier so they really overlap, wait for all
     * of them (without a timeout), and fail with the first error any of them raised (assertion
     * failures included).
     */
    protected void runConcurrently(int threads, final ThreadBody body) throws InterruptedException
    {
        final CyclicBarrier barrier = new CyclicBarrier(threads);
        final List<Throwable> errors = Collections.synchronizedList(new ArrayList<Throwable>());
        Thread[] T = new Thread[threads];
        for (int i = 0; i < threads; i++)
        {
            final int index = i;
            T[i] = new Thread(new Runnable() {
                public void run()
                {
                    try
                    {
                        barrier.await();
                        body.run(index);
                    }
                    catch (Throwable t)
                    {
                        errors.add(t);
                    }
                }
            }, "interim-test-" + i);
            T[i].start();
        }
        // No timeout: the threads are expected to finish. Every DIAGNOSTIC_INTERVAL_MINUTES, report
        // the threads still running and their stacks, to show where a slow run spends its time.
        long started = System.nanoTime();
        for (Thread t : T)
        {
            while (t.isAlive())
            {
                t.join(TimeUnit.MINUTES.toMillis(DIAGNOSTIC_INTERVAL_MINUTES));
                if (t.isAlive())
                    System.err.println(runningThreadsReport(T, started));
            }
        }
        if (!errors.isEmpty())
        {
            AssertionError e = new AssertionError(errors.size() + " thread(s) failed; first: " + errors.get(0));
            e.initCause(errors.get(0));
            throw e;
        }
    }

    private static String runningThreadsReport(Thread[] T, long startedNanos)
    {
        long minutes = TimeUnit.NANOSECONDS.toMinutes(System.nanoTime() - startedNanos);
        StringBuilder report = new StringBuilder("Concurrent test still running after " + minutes + " min:");
        for (Thread t : T)
            if (t.isAlive())
            {
                report.append("\n--- ").append(t.getName()).append(" (").append(t.getState()).append(")");
                for (StackTraceElement el : t.getStackTrace())
                    report.append("\n    at ").append(el);
            }
        return report.toString();
    }

    /**
     * Integrity sweep (test C7): every JsonProperty points to existing name and value atoms,
     * every array element exists, and every stored object can be materialized.
     */
    protected void checkIntegrity()
    {
        HGPersistentHandle nullHandle = graph.getHandleFactory().nullHandle();
        List<HGHandle> props = graph.findAll(hg.type(JsonProperty.class));
        for (HGHandle ph : props)
        {
            JsonProperty prop = graph.get(ph);
            assertNotNull("JsonProperty " + ph + ": name atom missing", graph.get(prop.getName()));
            if (!prop.getValue().equals(nullHandle))
                assertNotNull("JsonProperty " + ph + ": value atom missing " + prop.getValue(),
                              graph.get(prop.getValue()));
        }
        List<HGHandle> arrays = graph.findAll(hg.type(JsonTypeSchema.arrayTypeHandle));
        for (HGHandle ah : arrays)
        {
            Object x = graph.get(ah);
            if (!(x instanceof HGValueLink)) // an empty array is not wrapped in a link
                continue;
            for (HGHandle t : (HGValueLink)x)
                if (!t.equals(nullHandle))
                    assertNotNull("Array " + ah + ": element atom missing " + t, graph.get(t));
        }
        List<HGHandle> objects = graph.findAll(hg.type(JsonTypeSchema.objectTypeHandle));
        for (HGHandle oh : objects)
            assertNotNull("Object " + oh + " cannot be materialized", get(oh));
    }
}
