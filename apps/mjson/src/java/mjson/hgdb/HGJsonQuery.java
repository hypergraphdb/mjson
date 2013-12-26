package mjson.hgdb;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.query.And;
import org.hypergraphdb.query.impl.FilteredResultSet;
import org.hypergraphdb.util.Mapping;

import mjson.Json;

/**
 * <p>
 * Helper class to do pattern look in a JSON data set.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
class HGJsonQuery
{
    static abstract class Filter implements Mapping<Json, Boolean>
    {
        String propertyWithOp;
        String property;
        Filter(String fullProperty, String property) 
        { 
            this.propertyWithOp = fullProperty;
            this.property = property;
        }
    }
    
    static class RegExFilter extends Filter 
    {
        Pattern regex;
        public RegExFilter(String fullProperty, String property, String regex) 
        {
            super(fullProperty, property);
            this.regex = Pattern.compile(regex); 
        }
            
        public Boolean eval(Json value)
        {
            if (!value.isString())
                return false;
            else
                return regex.matcher(value.asString()).matches(); 
                    
        }
    }
    
    static Collection<Filter> collectFilters(Json pattern)
    {
        Set<Filter> S = new HashSet<Filter>();
        for (Map.Entry<String, Json> e : pattern.asJsonMap().entrySet())
        {
            String name = e.getKey();            
            if (Character.isLetterOrDigit(name.charAt(name.length() - 1)))
                continue;
            int at = name.length() - 1;
            while (!Character.isLetterOrDigit(name.charAt(at)))
                at--;
            String op = name.substring(at + 1);
            if (op.equals("~="))
                S.add(new RegExFilter(name, name.substring(0, at + 1), e.getValue().asString()));
            else
               ; // unknown operator are ignored and just remain part of the name of the JSON property
        }
        return S;
    }
    
    @SuppressWarnings("unchecked")
    static HGSearchResult<HGHandle> findObjectPattern(final HyperNodeJson node, Json pattern, boolean exact)
    {
        final Collection<Filter> filters = collectFilters(pattern);
        Mapping<HGHandle, Boolean> thefilter = null;
        if (!filters.isEmpty())
        {
            pattern = pattern.dup();
            for (Filter f : filters)
            {
                pattern.delAt(f.propertyWithOp);
            }
            thefilter = new Mapping<HGHandle, Boolean>()
            {
                public Boolean eval(HGHandle h)
                {
                    Json j = node.get(h);
                    for (Filter f : filters)
                    {
                        if (!f.eval(j.at(f.property)))
                            return false;
                    }
                    return true;
                }
            };
        }
        HGHandle [] A = new HGHandle[pattern.asJsonMap().size()];
        int i = 0;
        for (Map.Entry<String, Json> e : pattern.asJsonMap().entrySet())
        {
            HGHandle propHandle = node.findProperty(e.getKey(), e.getValue());                
            if (propHandle == null)
            {
                A = null;
                break;
            }
            A[i++] = propHandle;
        }
        if (A != null)
        {
            And and = hg.and(hg.type(JsonTypeSchema.objectTypeHandle), hg.link(A));
            if (exact) 
                and.add(hg.arity(i));
            HGSearchResult<HGHandle> rs = node.graph.find(and); 
            if (filters.isEmpty())
                return rs;
            else
                return new FilteredResultSet<HGHandle>(rs, thefilter, 0);
        }
        else
            return (HGSearchResult<HGHandle>) HGSearchResult.EMPTY;
        
    }
}
