package mjson.hgdb.querying;

import java.util.ArrayList;
import java.util.List;

import java.util.NoSuchElementException;
import org.hypergraphdb.HGSearchResult;

/**
 * 
 * <p>
 * Given a number of result sets, create a result set of tuples representing the
 * cross-product of the input result sets. 
 * </p>
 *
 * @author Borislav Iordanov
 *
 * @param <T>
 */
public class CrossProductResultSet<T> implements HGSearchResult<List<T>> 
{
	static class CursorState<T> 
	{
		HGSearchResult<T> rs;
		boolean direction; // true == forward, false == backward
	}
	
	private ArrayList<T> current, next, prev;
	private CursorState<T> [] components;
	private boolean initialized;
	private int index;
	
	private void initialize() 
	{
		if (initialized) return;
		index = -1;
		for (CursorState<T> cursor : components)
		{
			if (cursor.rs.hasNext()) 
				index++;
			else
				break;
			cursor.direction = true;
		}
		// if we did go through all components in the above loop, we know
		// the cross-product has at least one element
		if (index == components.length - 1) 
		{
			for (CursorState<T> cursor : components)
				cursor.rs.next();
			next = makeCurrent();
		}
		else // otherwise the cross-product is empty
			index = -1;
		initialized = true;
	}
	
	private boolean moveForward(CursorState<T> cursor)
	{
		if (cursor.direction)
		{
			if (cursor.rs.hasNext())
			{
				cursor.rs.next();
				return true;
			}
		}
		else
		{
			if (cursor.rs.hasPrev())
			{
				cursor.rs.prev();
				return true;
			}
		}
		return false;		
	}
	
	private void reverseDirection(int fromIndex)
	{
		while (fromIndex < components.length) {
			components[fromIndex].direction = !components[fromIndex].direction;
			fromIndex++;
		}
	}
	
	private ArrayList<T> makeCurrent()
	{
		ArrayList<T> result = new ArrayList<T>();
		for (CursorState<T> cursor : components)
			result.add(cursor.rs.current());
		return result;
	}
	
	private boolean advance() 
	{
		while (index > -1)
		{
			CursorState<T> cursor = components[index];
			if (moveForward(cursor))
			{
				// if we are not moving the last set, this means we need to start all
				// over from the last and enumerate all combination again before we
				// move onto the next element of the current set
				if (index < components.length - 1) 
				{
					reverseDirection(index + 1);
					index = components.length - 1;
				}
				break;
			}
			else
				index--;
		}
		if (index > -1)
		{
			next = makeCurrent();
			return true;
		}
		else
		{
			next = null;
			return false;
		}
	}
	
	private void back()
	{
		
	}
	
	@SuppressWarnings("unchecked")
	@SafeVarargs
	public CrossProductResultSet(HGSearchResult<?>...components)
	{	
		if (components == null)
			throw new NullPointerException("Attempt to construct a cross-product result set with null components.");
		this.initialized = false;
		this.components = new CursorState[components.length]; 
		for (int i = 0; i < this.components.length; i++)
		{
			this.components[i] = new CursorState<T>();
			this.components[i].rs = (HGSearchResult<T>)components[i];
		}
	}
	
	@Override
	public boolean hasPrev() 
	{
		return prev != null;
	}

	@Override
	public List<T> prev() 
	{
		if (!hasPrev())
			throw new NoSuchElementException();
		current = prev;
		back();
		return current;
	}

	@Override
	public boolean hasNext() 
	{
		if (!initialized)
			initialize();
		return next != null;
	}

	@Override
	public List<T> next() 
	{
		if (!hasNext())
			throw new NoSuchElementException();
		prev = current;
		current = next;
		advance();
		return current;
	}

	@Override
	public List<T> current() 
	{
		return current;
	}

	@Override
	public void close() 
	{
		for (CursorState<T> cursor : components)
			cursor.rs.close();
	}

	@Override
	public boolean isOrdered() 
	{
		return false;
	}

	@Override
	public void remove()
	{
		throw new UnsupportedOperationException();
	}
}