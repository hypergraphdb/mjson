package hgtest.mjsonapp.interim;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.hypergraphdb.HGPersistentHandle;
import org.junit.Test;

import mjson.Json;

/**
 * Regression scenarios from EXPERIMENT-NOTES.md that aren't already covered by a group test
 * (bug 1 is S3 and C1, the CME of bug 3 is C5, the notebook case is S1/S2).
 */
public class RegressionTests extends JsonModelTestBase
{
    /** Bug 3 side effect: a reader decorating its own dup() copy is not persisted by a writer's update. */
    @Test
    public void readerDecorationOnCopyIsNotPersisted()
    {
        final HGPersistentHandle h = add(PERSON_A);
        Json readerCopy = get(h).dup();
        readerCopy.set("decoration", "for-display");
        Json a = get(h);
        a.set("name", "Written");
        node.update(a);
        bothViews(() -> {
            assertEquals("Written", str(h, "name"));
            assertFalse("decoration must not be persisted", get(h).has("decoration"));
        });
    }

    /**
     * Bug 1 as described in the notes: trackers created with "parts": [] and parts added to one of them.
     * Passes on the current code because empty arrays are not deduplicated (X5a), so the trackers
     * don't actually share their [] atom. See the non-empty variant below.
     */
    @Test
    public void uploadTrackerPartsDoNotLeak()
    {
        final HGPersistentHandle t1 = add("{\"entity\":\"uploadTracker\",\"user\":\"u1\",\"parts\":[]}");
        final HGPersistentHandle t2 = add("{\"entity\":\"uploadTracker\",\"user\":\"u2\",\"parts\":[]}");
        Json tracker = get(t1);
        tracker.at("parts").add(Json.object("name", "part1"));
        node.update(tracker);
        tracker = get(t1);
        tracker.at("parts").add(Json.object("name", "part2"));
        node.update(tracker);
        bothViews(() -> {
            assertEquals(Json.read("[{\"name\":\"part1\"},{\"name\":\"part2\"}]"), at(get(t1), "parts"));
            assertEquals("the other tracker's parts must stay empty", Json.array(), at(get(t2), "parts"));
        });
    }

    /** Bug 1 with identical non-empty arrays, which are shared: parts added to one tracker must not leak. */
    @Test
    public void uploadTrackerNonEmptyPartsDoNotLeak()
    {
        final HGPersistentHandle t1 = add("{\"entity\":\"uploadTracker\",\"user\":\"u1\",\"parts\":[{\"name\":\"manifest\"}]}");
        final HGPersistentHandle t2 = add("{\"entity\":\"uploadTracker\",\"user\":\"u2\",\"parts\":[{\"name\":\"manifest\"}]}");
        assertEquals("precondition: parts shared", valueOf(t1, "parts"), valueOf(t2, "parts"));
        Json tracker = get(t1);
        tracker.at("parts").add(Json.object("name", "part1"));
        node.update(tracker);
        bothViews(() -> {
            assertEquals(Json.read("[{\"name\":\"manifest\"},{\"name\":\"part1\"}]"), at(get(t1), "parts"));
            assertEquals("the other tracker's parts must not change",
                         Json.read("[{\"name\":\"manifest\"}]"), at(get(t2), "parts"));
        });
    }
}
