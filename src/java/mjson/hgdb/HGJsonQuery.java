package mjson.hgdb;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.query.impl.FilteredResultSet;
import org.hypergraphdb.query.impl.PipedResult;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Mapping;

import mjson.Json;
import mjson.hgdb.querying.CrossProductResultSet;
import mjson.hgdb.querying.QueryHelp;

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
    static ThreadLocal<String> systemPrefix = new ThreadLocal<String>() {
        public String initialValue() { return "$"; }
    };
    
    static String sysPrefix()
    {
        return systemPrefix.get();
    }
    
    static String asSys(String propName) { return sysPrefix() + propName; }
    
    static double keywordScore(String text, String[] keywords)
    {
        StringTokenizer tokenizer = new StringTokenizer(text, " \t,.;?!~`@#$%^&*()-_+=\"[]{}:'<>/\\\n\r", false);
        int cnt = 0;
        int total = 0;
        while (tokenizer.hasMoreTokens())
        {
            for (int i = 0; i < keywords.length; i++)
                if (keywords[i].equalsIgnoreCase(tokenizer.nextToken()))
                    cnt++;
            total++;
        }
        return (double)cnt/(double)total;
    }
    
    static abstract class ItemMap implements Mapping<Json, Json> {}
    
    static class RegExFilter extends ItemMap 
    {
        String property;
        Pattern regex;
        
        public RegExFilter(String property, String regex) 
        {
            this.property = property;
            this.regex = Pattern.compile(regex); 
        }
            
        public Json eval(Json entity)
        {
            Json value = entity.at(property);
            if (!value.isString())
                return Json.nil();
            else if (regex.matcher(value.asString()).matches())
                return entity;
            else
                return Json.nil();                    
        }
    }
    
    static class KeywordMatch extends ItemMap
    {
        String property;
        String [] keywords;
        boolean assignScore;
        
        public KeywordMatch(String property, String []keywords, boolean assignScore) 
        {
            this.property = property;
            this.keywords = keywords; 
            this.assignScore = assignScore;
        }
            
        public Json eval(Json entity)
        {
            Json value = entity.at(property);
            if (!value.isString())
                return Json.nil();
            double score = keywordScore(value.asString(), this.keywords);
            if (score == 0)
                return Json.nil();
            if (assignScore)
            {
                Json existing = entity.at(asSys("score"));
                if (existing == null)
                    entity.set(asSys("score"), score);
                else   
                    entity.set(asSys("score"), existing.asDouble() + score);
            }
            return entity;
        }
    }
    
    static class PropertyOr extends ItemMap
    {
        Map<String, Json> condition;
        
        public PropertyOr(Map<String, Json> condition) { this.condition = condition; }
        
        public Json eval(Json entity)
        {
            for (Map.Entry<String, Json> e : condition.entrySet())
                if (entity.is(e.getKey(), e.getValue()))
                    return entity;
            return Json.nil();
        }
    }
    
    static ItemMap collectPropertyGroup(String name, Json pattern)
    {
        Map<String, Json> values = new HashMap<String, Json>();
        String [] parts = name.split(":");
        String operator = parts[0];
        String groupname = parts[1];
        values.put(parts[2], pattern.at(name));
        pattern.delAt(name);
        for (Map.Entry<String, Json> e : pattern.asJsonMap().entrySet())
        {
            String next = e.getKey();
            if (!next.startsWith(operator))
                continue;
            String [] nextParts = next.split(":");
            if (!nextParts[1].equals(groupname))
                continue;
            if (!nextParts[0].equals(operator))
                throw new IllegalArgumentException("Different operator " + nextParts[0] + 
                        " for logical grouping " + groupname + ", expecting " + parts[0]);
            values.put(nextParts[2], e.getValue());
            pattern.delAt(next);
        }
        return new PropertyOr(values);
    }
    
    @SuppressWarnings("unchecked")
    static Collection<ItemMap> collectMaps(Json pattern)
    {
        Set<ItemMap> S = new HashSet<ItemMap>();
        for (Map.Entry<String, Json> e : pattern.asJsonMap().entrySet())
        {
            String name = e.getKey();
            // If name starts with an operator, it spans multiple properties
            if (!Character.isLetter(name.charAt(0)))
            {
                S.add(collectPropertyGroup(name, pattern));
                continue;
            }
            if (Character.isLetterOrDigit(name.charAt(name.length() - 1)))
                continue;
            int at = name.length() - 1;
            while (!Character.isLetterOrDigit(name.charAt(at)))
                at--;
            String op = name.substring(at + 1);
            if (op.equals("~="))
            {
                S.add(new RegExFilter(name.substring(0, at + 1), e.getValue().asString()));
            }
            else if (op.equals("@="))
            {
                String [] keywords = null;
                if (e.getValue().isString())
                    keywords = e.getValue().asString().split("[ \t,]+");
                else if (e.getValue().isArray())
                    keywords = (String[])((List<String>)e.getValue().getValue()).toArray(new String[0]);
                if (keywords.length > 0)
                    S.add(new KeywordMatch(name.substring(0, at + 1), keywords, false));
            }
            else
            {
               // unknown operator are ignored and just remain part of the name of the JSON property
            }
            pattern.delAt(name);
        }
        return S;
    }
    
    @SuppressWarnings("unchecked")
    static HGSearchResult<HGHandle> findObjectPattern(final HyperNodeJson node, Json pattern, boolean exact)
    {
        pattern = pattern.dup();        
        final Collection<ItemMap> maps = collectMaps(pattern);
        Mapping<HGHandle, Boolean> themap = null;
        if (!maps.isEmpty())
        {
            themap = new Mapping<HGHandle, Boolean>()
            {
                public Boolean eval(HGHandle h)
                {
                    Json j = node.get(h);
                    for (ItemMap m : maps)
                    {
                        j = m.eval(j);
                        if (j.isNull())
                            return false;
                    }
                    return true;
                }
            };
        }
        HGSearchResult<HGHandle> [] propertyCandidates = new HGSearchResult[pattern.asJsonMap().size()];
        int i = 0;
        for (Map.Entry<String, Json> e : pattern.asJsonMap().entrySet())
        {
        	for (JsonProperty prop : QueryHelp.findAllProperties(node, e.getKey(), e.getValue()))
        		System.out.println(node.get(prop.getName()) + " = " + node.get(prop.getValue()));
        	propertyCandidates[i] = node.findPropertyPattern(e.getKey(), e.getValue());
        	if (!propertyCandidates[i].hasNext())
        	{
        		for (int j = i; j >= 0; j--)
        			HGUtils.closeNoException(propertyCandidates[j]);
        		propertyCandidates = null;
        		break;
        	}
        	i++;
        }
        if (propertyCandidates != null)        	
        {
            HGSearchResult<HGHandle> rs = new PipedResult<List<HGHandle>, HGHandle>(
            	new CrossProductResultSet<HGHandle>(propertyCandidates),
            	new QueryHelp.AbstractKeyBasedQuery<List<HGHandle>, HGHandle>()
            	{
        			public HGSearchResult<HGHandle> execute()
        			{
        				return node.find(hg.and(hg.type(JsonTypeSchema.objectTypeHandle), hg.link(getKey()))); 
        			} 
            	},
            	true
            ); 
            		
//            		node.graph.find(and); 
            if (maps.isEmpty())
                return rs;
            else
                return new FilteredResultSet<HGHandle>(rs, themap, 0);
        }
        else
            return (HGSearchResult<HGHandle>) HGSearchResult.EMPTY;
        
    }
}