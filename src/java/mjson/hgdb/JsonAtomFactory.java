package mjson.hgdb;

import mjson.Json;
import mjson.Json.Factory;

public class JsonAtomFactory implements Factory
{
    public Json make(Object anything)
    {
        return Json.defaultFactory.make(anything);
    }
    
    public Json nil()
    {
        return Json.defaultFactory.nil();
    }

    public Json bool(boolean value)
    {
        return Json.defaultFactory.bool(value);
    }

    public Json string(String value)
    {
        return Json.defaultFactory.string(value);
    }

    public Json number(Number value)
    {
        return Json.defaultFactory.number(value);
    }

    public Json object()
    {
        return new ArrayJsonHGDB();
    }

    public Json array()
    {
        return new ObjectJsonHGDB();
    }
}