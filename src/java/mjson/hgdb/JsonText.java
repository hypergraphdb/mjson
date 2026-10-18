package mjson.hgdb;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import mjson.Json;

/**
 * JSON text of the objects and arrays implemented in this package.
 */
final class JsonText
{
    private JsonText() { }

    static String object(Map<String, Json> properties, int maxCharacters)
    {
        StringBuilder sb = new StringBuilder("{");
        for (Iterator<Map.Entry<String, Json>> i = properties.entrySet().iterator(); i.hasNext(); )
        {
            Map.Entry<String, Json> e = i.next();
            sb.append(Json.make(e.getKey()).toString()).append(':').append(e.getValue().toString(maxCharacters));
            if (i.hasNext())
                sb.append(',');
            if (sb.length() >= maxCharacters)
                return truncate(sb, maxCharacters, "...}");
        }
        return sb.append('}').toString();
    }

    static String array(List<Json> elements, int maxCharacters)
    {
        StringBuilder sb = new StringBuilder("[");
        for (Iterator<Json> i = elements.iterator(); i.hasNext(); )
        {
            sb.append(i.next().toString(maxCharacters));
            if (i.hasNext())
                sb.append(',');
            if (sb.length() >= maxCharacters)
                return truncate(sb, maxCharacters, "...]");
        }
        return sb.append(']').toString();
    }

    private static String truncate(StringBuilder sb, int maxCharacters, String ending)
    {
        if (sb.length() > maxCharacters)
            sb.setLength(maxCharacters);
        return sb.append(ending).toString();
    }
}
