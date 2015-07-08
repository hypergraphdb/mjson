package mjson.hgdb;

import java.util.Iterator;

import mjson.Json;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.type.HGCompositeType;
import org.hypergraphdb.type.HGProjection;

public class JsonType implements HGCompositeType
{
	private HyperGraph graph;
	
	public Iterator<String> getDimensionNames()
	{
		// TODO Auto-generated method stub
		return null;
	}

	public HGProjection getProjection(String dimensionName)
	{
		// TODO Auto-generated method stub
		return null;
	}

	public Object make(HGPersistentHandle handle,
			LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet)
	{
		// TODO Auto-generated method stub
		return null;
	}

	public void release(HGPersistentHandle handle)
	{
		// TODO Auto-generated method stub

	}

	public HGPersistentHandle store(Object instance)
	{
		Json j = (Json)instance;
		return null;
	}

	public boolean subsumes(Object general, Object specific)
	{
		// TODO Auto-generated method stub
		return false;
	}

	public void setHyperGraph(HyperGraph graph)
	{
		this.graph = graph;
	}

}
