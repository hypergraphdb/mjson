package hgtest.mjsonapp;


import java.util.List;

import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Mapping;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import hgtest.HGTestBase;
import hgtest.T;
import mjson.Json;
import mjson.hgdb.EntityInterfaceImpl;
import mjson.hgdb.HyperNodeJson;
import mjson.hgdb.JsonTypeSchema;
import static mjson.hgdb.Helpers.*;

public class JsonStorageTests extends HGTestBase
{
    static HyperNodeJson node;

    static class RemoveProp implements Mapping<Json, Boolean>
    {
        String name;
        public RemoveProp(String name) { this.name = name; }
        public Boolean eval(Json j) { if (j.isObject()) j.delAt(name); return true; }
    }

    static void openNode()
    {
        graph = HGEnvironment.get(getGraphLocation(), config);
        node = new HyperNodeJson(graph);    	
    }
    
    static void closeNode()
    {
    	graph.close();
    }
    
    void reopen()
    {
        tearDown();
        openNode();
    }

    @BeforeClass
    public static void setUp()
    {
        HGUtils.dropHyperGraphInstance(getGraphLocation());    	
        config = new HGConfiguration();
        config.getTypeConfiguration().addSchema(new JsonTypeSchema());
        openNode();
    }

    @AfterClass
    public static void tearDown()
    {
    	closeNode();
    }
    
    @Test
    public void testPrimitives()
    {
        // All primitives should be asserted.

        // null
        HGHandle n1 = node.add(Json.nil());
        Assert.assertNotNull(n1);
        HGHandle n2 = node.add(Json.nil());
        Assert.assertEquals(n1, n2);

        // boolean
        HGHandle b1 = node.add(Json.make(true));
        HGHandle b2 = node.add(Json.make(true));
        Assert.assertEquals(b1, b2);

        // integer
        HGHandle i1 = node.add(Json.make(42));
        HGHandle i2 = node.add(Json.make(42));
        Assert.assertEquals(i1, i2);
        
        // real
        HGHandle r1 = node.add(Json.make(4265.90384));
        HGHandle r2 = node.add(Json.make(4265.90384));
        Assert.assertEquals(r1, r2);

        // string
        node.add(Json.make("gmdsfgm398rjga;83fja8gjq3pg"));
        HGHandle s2 = node.add(Json.make("gmdsfgm398rjga;83fja8gjq3pg"));
        Assert.assertEquals(r1, r2);

        reopen();
        Assert.assertEquals(n2, node.exactly(Json.nil()));
        Assert.assertEquals(b1, node.exactly(Json.make(true)));
        Assert.assertEquals(i2, node.exactly(Json.make(42)));
        Assert.assertEquals(r2, node.exactly(Json.make(4265.90384)));
        Assert.assertEquals(s2, node.exactly(Json.make("gmdsfgm398rjga;83fja8gjq3pg")));
    }

    @Test
    public void testObjectAdd()
    {
        // Adding non-entity objects should also assert
        Json o = Json.object("name", "HyperGraphDB", "nosql", true, "year", 2004, "parent", null);
        HGHandle h = node.add(o);
        HGHandle h2 = node.add(o.dup());
        Assert.assertFalse(o.has("hghandle"));
        Assert.assertEquals(h, h2);
    }

    @Test
    public void testObjectUpdate()
    {
    	Json o = Json.object("name", "Dusty", 
    						 "position", 5.45, 
    						 "age", 55, 
    						 "entity", "user",
    						 "address", Json.object("street", "35 Bloom", 
    								 				"properties", Json.object(),
    								 			    "city", Json.object("name", "New York",
    								 			    					"properties", Json.nil(),
    								 			    					"population", 10000000)));
    	HGHandle h = node.add(o);
    	Json newo = o.dup();
    	newo.set("name", "Dustier");
    	node.replace(h, newo, node.getType(h));
    	reopen();
    	Json readBack = node.get(h.getPersistent());
    	Assert.assertEquals(newo, readBack);
    }
    
    @Test
    public void testAssertNumber()
    {
        // The smallest number expressible as the sum of cubes in two different ways
        Json num = Json.make(1729);
        HGHandle h = node.add(num);
        Assert.assertEquals(h, node.add(num));
        Assert.assertEquals(h, node.exactly(num));        
        reopen();
        Assert.assertEquals(h, node.exactly(num));        
        Assert.assertEquals(h, node.add(num));
    }

    @Test
    @Ignore
    public void testAddCircular()
    {
    	Json ref1 = Json.object("n", 1);
    	Json ref2 = Json.object("n", 2);
    	ref1.set("ref", ref2);
    	ref2.set("ref", ref1);
    	HGHandle handle = node.add(ref1).getPersistent();
    	reopen();
    	Json ref1_back = node.get(handle);
    	Assert.assertEquals(ref1, ref1_back);
    }
    
    @Test
    public void testAddEntity()
    {
        // Adding an entity should create a new atom each time.
        Json entity = Json.object("entity", "book", 
                                  "title", "Better Yourself", 
                                  "author", "Schmuck Guru", 
                                  "year", 1987);
        HGHandle h1 = node.add(entity);
        Assert.assertNotNull(h1);
        Assert.assertTrue(entity.has("hghandle"));
        // Adding it again should result in the same handle since it's in the cache
        Assert.assertEquals(node.add(entity), h1);
        // Adding a clone should add a new atom
        Json entity2 = entity.dup().delAt("hghandle");
        HGHandle h2 = node.add(entity2);
        Assert.assertNotNull(h2);
        Assert.assertTrue(entity2.has("hghandle"));
        Assert.assertFalse(h1.equals(h2));
    }

    @Test
    public void testComplexEntity()
    {
        // The following is an entity that has some other entities nested in it, at various
        // levels. It also has some values that should be immutable and not duplicated in 
        // the database.
        Json object = Json.read(T.getResourceContents("/hgtest/mjsonapp/data1.json"));
        node.add(object);
        Assert.assertTrue(object.has("hghandle"));
        // Nested entity should also have a handle:
        traverse(object, new Mapping<Json, Boolean>() { public Boolean eval(Json j) 
                { if (j.isObject() && j.has("entity")) Assert.assertTrue(j.has("hghandle")); return true; } 
        });

        // There is still more work to do on spec-ing the entity management. The decision
        // to forbid entities inside a value doesn't seem to square well with some sensible
        // use cases, like having an array of entities as a property of an enclosing entity -
        // the array is immutable to that prevents us from having entities inside it, which doesn't
        // make much sense. We need different rules before this and other similar tests can be completed.

        // ... and be added as a separate atom
        //        Assert.assertNotEquals(object.at("owns").at(0).at("hghandle"), object.at("owns").at(1).at("hghandle"));

        // except when there is a primary key match for this kind of entity
        //        Assert.assertNotEquals(object.at("spouse").at("hghandle"), object.at("watching").at(0).at("star"));

        // Nested value should not have handle
        Assert.assertFalse(object.at("stats").has("hghandle"));

        /*        reopen();
        Json fromdb = node.get(he);
        fromdb = traverse(fromdb.dup(), new RemoveProp("hghandle"));
        Assert.assertTrue(fromdb.equals(object));
        fromdb = node.get(he);
        Assert.assertEquals(traverse(fromdb.dup(), new RemoveProp("hghandle")), object);
        Assert.assertEquals(node.add(object), he);         */
        
        List<Json> L = node.getAll(
        		Json.object("entity", "user",
    					"username", "morbo",
    					"stats", Json.object("friends", 25))).asJsonList();
        System.out.println(L.size());
//        HGHandle h1 = node.match(Json.object("sound", true), false);
//        HGHandle propHandle = node.findProperty("stats", h1);
        Assert.assertTrue(L.size() >= 1);
        Assert.assertTrue(node.getAll(
        		Json.object("entity", "user",
        					"username", "morbo",
        					"stats", Json.object("sound", true))).asJsonList().size() >= 1);

    }

    @Test
    public void testEntityInValue()
    {
    	// In an "entity in value" situation, our entity interface is configured to allow
    	// mutable entities into immutable values and we want to just store the reference
    	// of the entity.
    	((EntityInterfaceImpl)node.getEntityInterface()).allowEntitiesInImmutableValues(true);
    	Json e1 = Json.object("entity", "thing", "label", "My Thing 1");
    	HGHandle h = node.add(e1).getPersistent();
    	Json value = Json.object("prop1", 10, 
    							 "prop2", false, 
    							 "myentity", e1);
    	HGHandle valueHandle = node.add(value).getPersistent();
    	this.reopen();
    	value = node.get(valueHandle);
    	System.out.println(value);
    }

    @Test
    public void storeEmptyObject()
    {
    	Json empty = Json.object();
    	HGHandle h = node.add(empty).getPersistent();
    	Assert.assertEquals(Json.object(), node.get(h));
    	Json withEmpty = Json.object("type", "bla", "nested", Json.object());
    	h = node.add(withEmpty).getPersistent();
    	withEmpty.set("type", "foo");
    	node.update(withEmpty);
    	this.reopen();
    	Assert.assertEquals(Json.object("type", "foo", "nested", Json.object()), node.get(h));
    }
    
    public static void main(String[] argv)
    {
        JsonStorageTests test = new JsonStorageTests();
        try
        {
        	JsonStorageTests.setUp();
        	test.testObjectUpdate();
            test.storeEmptyObject();
            System.out.println("test passed successfully");
        }
        catch (Throwable t)
        {
            t.printStackTrace(System.err);
        }
        finally
        {
        	JsonStorageTests.tearDown();
        }
    }
}
