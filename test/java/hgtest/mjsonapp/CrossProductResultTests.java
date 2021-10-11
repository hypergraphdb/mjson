package hgtest.mjsonapp;

import java.io.IOException;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import org.assertj.core.util.Lists;
import org.hypergraphdb.HGEnvironment;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.util.HGUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import hgtest.T;
import mjson.hgdb.querying.CrossProductResultSet;

public class CrossProductResultTests
{
	static HyperGraph graph;
	
	String toString(List<HGHandle> tuple)
	{
		StringBuilder sb = new StringBuilder("[");
		Iterator<HGHandle> I = tuple.iterator();
		while (I.hasNext())
		{
			sb.append(graph.get(I.next()).toString());
			if (I.hasNext()) sb.append(", ");
		}
		sb.append("]");
		return sb.toString();
	}
	
	@BeforeClass
	public static void initGraph() throws IOException
	{
		graph = HGEnvironment.get(Files.createTempDirectory("hgtest").toFile().getAbsolutePath());
	}
	
	@AfterClass
	public static void dropGraph()
	{
		if (graph != null)
			graph.close();
		HGUtils.dropHyperGraphInstance(graph.getLocation());
	}
	
	@Test
	//@Ignore
	public void testEmptyProduct()
	{
		try (CrossProductResultSet<HGHandle> rs = new CrossProductResultSet<HGHandle>(
			graph.find(hg.type(String.class)),
			graph.find(hg.type(Integer.class)),
			graph.find(hg.type(Double.class))
		))
		{
			Assert.assertFalse(rs.hasNext());
			Assert.assertFalse(rs.hasPrev());
		}
	}
	
	@Test
	//@Ignore
	public void testSingleResult()
	{
		final HashSet<HGHandle> atoms = new HashSet<HGHandle>(); 
		graph.getTransactionManager().transact(new Callable<Object>() {
			public Object call()
			{
				for (int i = 0; i < 10; i++)
					atoms.add(graph.add("value " + i));
				return null;
			}
		});		
		
		try (CrossProductResultSet<HGHandle> rs = new CrossProductResultSet<HGHandle>(
				graph.find(hg.type(String.class))
			))
			{
				while (rs.hasNext())
				{
					System.out.println((String) graph.get(rs.next().get(0)));
					atoms.remove(rs.current().get(0));
				}
			}
		Assert.assertEquals(0, atoms.size());
	}
	
	@Test
	//@Ignore
	public void testBiProduct()
	{
		// First, populate some data for two different types
		graph.getTransactionManager().transact(new Callable<Object>() {
			public Object call()
			{
				for (int i = 0; i < 4; i++)
					graph.add("value " + i);
				for (int i = 0; i < 7; i++)
					graph.add(new Double(Math.random()));
				return null;
			}
		});
		
		// This should be empty because we have no floats in the DB.
		try (CrossProductResultSet<HGHandle> rs = new CrossProductResultSet<HGHandle>(
				graph.find(hg.type(String.class)),
				graph.find(hg.type(Float.class)),
				graph.find(hg.type(Double.class))
			))
			{
				Assert.assertFalse(rs.hasNext());
				Assert.assertFalse(rs.hasPrev());
			}
		
		Set<List<HGHandle>> tuples = new HashSet<List<HGHandle>>();
		List<HGHandle> strings = graph.findAll(hg.type(String.class));
		List<HGHandle> doubles = graph.findAll(hg.type(Double.class));
		for (HGHandle sh : strings)
			for (HGHandle dh : doubles)
				tuples.add(Lists.newArrayList(sh, dh));
		Set<List<HGHandle>> tuples2 = new HashSet<List<HGHandle>>();
		tuples2.addAll(tuples);
		try (CrossProductResultSet<HGHandle> rs = new CrossProductResultSet<HGHandle>(
				graph.find(hg.type(String.class)),
				graph.find(hg.type(Double.class))
			))
			{
//				Assert.assertFalse(rs.hasNext());
//				Assert.assertFalse(rs.hasPrev());
				while (rs.hasNext())
				{					
					Assert.assertTrue(tuples.contains(rs.next()));
					tuples.remove(rs.current());
					System.out.println(toString(rs.current()));
				}
				Assert.assertEquals(0, tuples.size());
				
				tuples.add(rs.current()); // "first previous"
				
				while (rs.hasPrev())
				{
					tuples.add(rs.prev());
					System.out.println(toString(rs.current()));
				}
				
				Assert.assertEquals(tuples2, tuples);
			}
	}

	@Test
	public void testManyProduct()
	{
		// First, populate some data for two different types
		graph.getTransactionManager().transact(new Callable<Object>() {
			public Object call()
			{
				for (int i = 0; i < 4; i++)
					graph.add("value " + i);
				for (int i = 0; i < 7; i++)
					graph.add(new Double(Math.random()));
				for (int i = 0; i < 3; i++)
					graph.add(new Integer(T.random(Integer.MAX_VALUE)));
				return null;
			}
		});
		
		// This should be empty because we have no floats in the DB.
		try (CrossProductResultSet<HGHandle> rs = new CrossProductResultSet<HGHandle>(
				graph.find(hg.type(String.class)),
				graph.find(hg.type(Integer.class)),
				graph.find(hg.type(Float.class)),
				graph.find(hg.type(Double.class))
			))
			{
				Assert.assertFalse(rs.hasNext());
				Assert.assertFalse(rs.hasPrev());
			}
		
		Set<List<HGHandle>> tuples = new HashSet<List<HGHandle>>();
		List<HGHandle> strings = graph.findAll(hg.type(String.class));
		List<HGHandle> integers = graph.findAll(hg.type(Integer.class));
		List<HGHandle> doubles = graph.findAll(hg.type(Double.class));
		for (HGHandle sh : strings)
			for (HGHandle dh : doubles)
				for (HGHandle ih : integers)
					tuples.add(Lists.newArrayList(sh, dh, ih));
		Set<List<HGHandle>> tuples2 = new HashSet<List<HGHandle>>();
		tuples2.addAll(tuples);
		try (CrossProductResultSet<HGHandle> rs = new CrossProductResultSet<HGHandle>(
				graph.find(hg.type(String.class)),
				graph.find(hg.type(Double.class)),
				graph.find(hg.type(Integer.class))
			))
			{
//				Assert.assertFalse(rs.hasNext());
//				Assert.assertFalse(rs.hasPrev());
				while (rs.hasNext())
				{					
					Assert.assertTrue(tuples.contains(rs.next()));
					tuples.remove(rs.current());
					System.out.println(toString(rs.current()));
				}
				Assert.assertEquals(0, tuples.size());
				
				tuples.add(rs.current()); // "first previous"
				
				while (rs.hasPrev())
				{
					tuples.add(rs.prev());
					System.out.println(toString(rs.current()));
				}
				
				Assert.assertEquals(tuples2, tuples);
			}		
	}
}