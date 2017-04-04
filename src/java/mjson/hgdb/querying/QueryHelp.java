package mjson.hgdb.querying;

import java.util.ArrayList;
import java.util.List;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.query.impl.KeyBasedQuery;
import org.hypergraphdb.query.impl.PipedResult;

import mjson.hgdb.HyperNodeJson;
import mjson.hgdb.JsonProperty;

/**
 * 
 * <p>
 * WIP...fooling around with patterns
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class QueryHelp
{
	@SuppressWarnings("unchecked")
	public static <T> HGRandomAccessResult<T> empty()
	{
		return (HGRandomAccessResult<T>) HGSearchResult.EMPTY;
	}

	public static <T> HGSearchResult<T> pipeCrossProductToCompiledQuery(
			final CrossProductResultSet<T> crossProduct,
			final HGQuery<T> compiledQuery, final String... varnames)
	{
		KeyBasedQuery<List<T>, T> propQuery = new KeyBasedQuery<List<T>, T>()
		{
			List<T> key = null;

			public void setKey(List<T> key)
			{
				this.key = key;
			}

			@Override
			public List<T> getKey()
			{
				return this.key;
			}

			@Override
			public HGSearchResult<T> execute()
			{
				for (int i = 0; i < varnames.length; i++)
					compiledQuery.var(varnames[i], key.get(i));
				return compiledQuery.execute();
			}
		};
		return new PipedResult<List<T>, T>(crossProduct, propQuery, true);
	}

	public static <T> HGSearchResult<T> pipeCrossProductToQuery(final CrossProductResultSet<T> crossProduct,
																final HGQuery<T> compiledQuery, 
																final String... varnames)
	{
		KeyBasedQuery<List<T>, T> propQuery = new KeyBasedQuery<List<T>, T>()
		{
			List<T> key = null;

			public void setKey(List<T> key)
			{
				this.key = key;
			}

			@Override
			public List<T> getKey()
			{
				return this.key;
			}

			@Override
			public HGSearchResult<T> execute()
			{
				for (int i = 0; i < varnames.length; i++)
					compiledQuery.var(varnames[i], key.get(0));
				return compiledQuery.execute();
			}
		};
		return new PipedResult<List<T>, T>(crossProduct, propQuery, true);
	}

	public static abstract class AbstractKeyBasedQuery<Key, Value> extends KeyBasedQuery<Key, Value>
	{
		Key key = null;

		public void setKey(Key key)
		{
			this.key = key;
		}

		@Override
		public Key getKey()
		{
			return this.key;
		}
	};	
	
	public static List<JsonProperty> findAllProperties(HyperNodeJson node, String namePattern, Object valuePattern)
	{
		List<JsonProperty> L = new ArrayList<JsonProperty>();
		try (HGSearchResult<HGHandle> rs = node.findPropertyPattern(namePattern, valuePattern))
		{
			while (rs.hasNext())
			{
				JsonProperty prop = node.get(rs.next());
				L.add(prop);
			}
		}
		return L;
	}
}
