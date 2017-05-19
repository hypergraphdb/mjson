package hgtest.mjsonapp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGSearchResult;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import hgtest.HGTestBase;
import hgtest.T;

import org.junit.Assert;

import mjson.Json;
import static mjson.Json.*;
import static mjson.hgdb.Helpers.resolveEntities;
import mjson.hgdb.HyperNodeJson;
import mjson.hgdb.JsonTypeSchema;

/**
 * <p>
 * Tests for various query patterns. 
 * </p>
 *
 * This is a good place to generate test data for JSON:
 * http://www.json-generator.com/
 * 
 * @author Borislav Iordanov
 *
 */
public class JsonQueryTests extends HGTestBase
{
    static HyperNodeJson node;

    static Json matchOne(Json pattern, boolean exact)
    {
    	HGHandle h = node.match(pattern, exact);
    	Assert.assertNotNull("Failed to match " + pattern, h);
    	return node.get(h);
    }
    
    static Collection<Json> matchMany(Json pattern, boolean exact)
    {
    	ArrayList<Json> L = new ArrayList<Json>();
    	try (HGSearchResult<HGHandle> rs = node.find(pattern, exact))
    	{
    		L.add((Json)node.get(rs.next()));
    	}
    	return L;
    }
    
    static void load_one(String resource)
    {
        Json object = Json.read(T.getResourceContents(resource));
        node.add(object);    	
    }

    static void load_many(String resource)
    {
        Json A = Json.read(T.getResourceContents(resource));
        if (!A.isArray())
        	throw new RuntimeException("Expected a JSON array in resource: " + resource);
        for (Json element : A.asJsonList())
        	node.add(element);
    }
    
	@BeforeClass
	public static void setUp()
	{
        config = new HGConfiguration();
        config.getTypeConfiguration().addSchema(new JsonTypeSchema());
        graph = HGEnvironment.get(getGraphLocation(), config);
        node = new HyperNodeJson(graph);        
        node.getEntityInterface().allowEntitiesInImmutableValues(true);
        load_many("/hgtest/mjsonapp/primitives.json");
        load_many("/hgtest/mjsonapp/dataset1.json");
        load_one("/hgtest/mjsonapp/data1.json");
        load_many("/hgtest/mjsonapp/arrays.json");
	}
	
	@Test
	public void testSearchPrimitives()
	{
		Json primitives = Json.array(
		  1111,
		  0.1111,
		  true,
		  false,
		  "primitive1",
		  "primitive2",
		  "PriMitiVe1");
		for (Json p : primitives.asJsonList())
		{
			HGHandle found = node.match(p, true);
			Assert.assertNotNull(found);
			Assert.assertEquals(p,  node.get(found));
		}
	}
	
	@Test
	public void testMatchObjectByOneProperty()
	{
		Json j = matchOne(Json.object("username", "morbo"), false);
		Assert.assertTrue(j.is("age", 41));
		Assert.assertTrue(j.is("vip", false));
		Assert.assertTrue(j.is("favoriteMovie", Json.object()));
	}

	@Test
	public void testMatchSeveralNestedProperties()
	{
		Json pattern = Json.object("spouse", Json.object("full_name", "Asko Tedesco"));
		Json j = matchOne(pattern, false); 
		Assert.assertTrue(j.is("age", 41));
		Assert.assertTrue(j.is("vip", false));
		Assert.assertTrue(j.is("favoriteMovie", Json.object()));
		
		j = matchOne(Json.object("stats", 
			Json.object("friends", 25,
			            "shows", Json.object("library", 169, "watched", 93))), false);

		Assert.assertTrue(j.is("age", 41));
		Assert.assertTrue(j.is("vip", false));
		Assert.assertTrue(j.is("favoriteMovie", Json.object()));
		
	}

	@Test
	public void testMatchEntity()
	{
		Json person = node.retrieve(object("entity", "person", "firstName", "Joe"));
		Assert.assertEquals("Bonny", person.at("lastName").asString());
		
		Json store = node.retrieve(object("entity", "store", "active", true));
		Assert.assertTrue(store.has("manager"));
		Assert.assertTrue(store.has("company"));
		
		Json L = node.getAll(object("entity", "store", "manager", object("firstName", "Joe")));
    	L = resolveEntities(node, L);
		Assert.assertTrue(L.at(0).at("manager").is("lastName", "Bonny"));
		System.out.println(L);
	}
	
	@Test
	public void testMatchArray()
	{
		Json arrayPattern = Json.array(
			Json.object("color", "red", "value", "#f00"),
			Json.object("color", "green", "value", "#0f0"));
		Assert.assertNotNull(node.match(arrayPattern, false));
		Assert.assertNull(node.match(arrayPattern, true));
	}
	
	@Test
	public void testMatchArrayPattern()
	{
		Json person1 = Json.object(
		    "gender", "male"
		);
		Json person2 = Json.object(
			"balance", "$2,437.15"				
		);
		Json arrayPattern = Json.array(person1, person2);
		Assert.assertNotNull(node.match(arrayPattern, false));
//		Assert.assertNull(node.match(arrayPattern, true));		
	}
	
	@Test
	public void testMatchRegExPattern()
	{
		
	}
	
    public static void main(String[] argv)
    {
    	Json A = Json.read(T.getResourceContents("/hgtest/mjsonapp/arrays.json"));
    	/*
        JsonQueryTests test = new JsonQueryTests();
        try
        {
        	JsonQueryTests.setUp();
            test.testMatchSeveralNestedProperties();
            System.out.println("test passed successfully");
        }
        catch (Throwable t)
        {
            t.printStackTrace(System.err);
        }
        finally
        {
        	JsonStorageTests.tearDown();
        } */
    }
	
}
