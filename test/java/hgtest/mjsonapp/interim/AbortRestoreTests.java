package hgtest.mjsonapp.interim;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.concurrent.atomic.AtomicInteger;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.transaction.TransactionConflictException;
import org.junit.Test;

import mjson.Json;
import mjson.hgdb.HGObjectJson;

/**
 * Group T of docs/test-plan-interim.md: an entity mutated inside a transaction that aborts gets its
 * pre-transaction state back (storage-model-plan.md, section 3.4).
 */
public class AbortRestoreTests extends JsonModelTestBase
{
    static class Rollback extends RuntimeException
    {
        private static final long serialVersionUID = 1L;
    }

    /** T1: top-level mutation inside an aborted transaction is undone. */
    @Test
    public void t1_topLevelMutationRestoredOnAbort()
    {
        final HGPersistentHandle h = add(PERSON_A);
        final Json canonical = get(h);
        expectException(() -> tx(() -> {
            Json a = get(h);
            a.set("name", "InTx");
            throw new Rollback();
        }), Rollback.class);
        assertSame(canonical, get(h));
        bothViews(() -> assertEquals("Assia", str(h, "name")));
    }

    /** T2: mutation through an access path inside an aborted transaction is undone. */
    @Test
    public void t2_pathMutationRestoredOnAbort()
    {
        final HGPersistentHandle h = add(PERSON_A);
        expectException(() -> tx(() -> {
            Json a = get(h);
            a.at("address").set("street", "InTx");
            throw new Rollback();
        }), Rollback.class);
        bothViews(() -> assertEquals("S1", str(h, "address", "street")));
    }

    /** T3: a transaction retried after a conflict starts from the pre-transaction state. */
    @Test
    public void t3_retryStartsFromPreTransactionState()
    {
        final HGPersistentHandle h = add(PERSON_A);
        final AtomicInteger attempts = new AtomicInteger();
        tx(() -> {
            Json a = get(h);
            a.at("tags").add(Json.make("once"));
            if (attempts.incrementAndGet() == 1)
                throw new TransactionConflictException();
            node.update(a);
            return null;
        });
        assertEquals("precondition: the transaction was retried", 2, attempts.get());
        bothViews(() -> assertEquals(Json.read("[\"t\",\"once\"]"), at(get(h), "tags")));
    }

    // T4 (nested transactions) is not applicable: the BerkeleyDB JE storage rejects nested
    // transactions ("Nested transaction detected. Not supported by JE Berkeley DB.").

    /**
     * T5 LIMIT (pitfall P10), informational: a mutation made outside any transaction followed by an
     * update in a transaction that aborts. Storage must be unchanged; what memory shows is reported.
     * (On the current code the rollback replaces the cached object, so get() returns a different
     * object than the one the caller mutated; see T6.)
     */
    @Test
    public void t5_limit_mutationOutsideTransactionThenAbortedUpdate()
    {
        final HGPersistentHandle h = add(PERSON_A);
        final Json a = get(h);
        a.set("name", "Outside");
        expectException(() -> tx(() -> {
            node.update(a);
            throw new Rollback();
        }), Rollback.class);
        Json after = get(h);
        System.out.println("t5 (LIMIT, informational): caller's object name=" + a.at("name")
                           + ", get() name=" + after.at("name") + ", same object=" + (after == a));
        reopen();
        assertEquals("storage: unchanged", "Assia", str(h, "name"));
    }

    /**
     * T7: a transaction that adds an entity, changes it, then aborts: the caller's object goes back
     * to being a plain object with its original content, and nothing is stored.
     */
    @Test
    public void t7_abortedAddRevertsPromotion()
    {
        final Json obj = Json.read(PERSON_A);
        final Json original = obj.dup();
        final HGHandle[] added = new HGHandle[1];
        expectException(() -> tx(() -> {
            added[0] = node.add(obj);
            obj.set("name", "InTx");
            obj.at("address").set("street", "InTx");
            throw new Rollback();
        }), Rollback.class);
        assertFalse("back to a plain object", ((HGObjectJson)obj).isEntityObject());
        assertEquals(original, obj);
        obj.set("name", "plain again"); // a plain object is freely mutable
        assertEquals("plain again", obj.at("name").asString());
        assertNull("nothing stored", get(added[0]));
    }

    /** T6: an aborted update keeps the canonical object: get() still returns the same Java object. */
    @Test
    public void t6_abortedUpdateKeepsCanonicalObject()
    {
        final HGPersistentHandle h = add(PERSON_A);
        final Json canonical = get(h);
        expectException(() -> tx(() -> {
            Json a = get(h);
            a.set("name", "InTx");
            node.update(a);
            throw new Rollback();
        }), Rollback.class);
        assertSame("get() must return the same canonical object after an aborted update", canonical, get(h));
        assertEquals("Assia", canonical.at("name").asString());
    }
}
