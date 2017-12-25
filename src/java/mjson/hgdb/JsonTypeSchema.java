package mjson.hgdb;

import java.net.URI;

import mjson.Json;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.handle.UUIDHandleFactory;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.type.HGTypeSchema;

public final class JsonTypeSchema implements HGTypeSchema<Object>
{
    private HyperGraph graph;
    private HGTypeSchema<? extends Object> wrapped;
    
    public JsonTypeSchema()
    {        
    }
    
    public JsonTypeSchema(HGTypeSchema<? extends Object> wrapped)
    {
        this.wrapped = wrapped;
    }
    
    public String getName()
    {
        return "json";
    }

    public void initialize(HyperGraph graph)
    {        
        this.graph = graph;
        if (wrapped == null)
            this.wrapped = graph.getConfig().getTypeConfiguration().getDefaultSchema();
        try
        {
            graph.getTypeSystem().addPredefinedType(booleanTypeHandle.getPersistent(), 
                                                    new BoolTypeJson(), 
                                                    new URI("json:boolean"));
            graph.getTypeSystem().addPredefinedType(nullTypeHandle.getPersistent(), 
                                                    new NullTypeJson(), 
                                                    new URI("json:null"));
            graph.getTypeSystem().addPredefinedType(numberTypeHandle.getPersistent(), 
                                                    new NumberTypeJson(), 
                                                    new URI("json:number"));
            graph.getTypeSystem().addPredefinedType(stringTypeHandle.getPersistent(), 
                                                    new StringTypeJson(), 
                                                    new URI("json:string"));
            graph.getTypeSystem().addPredefinedType(arrayTypeHandle.getPersistent(), 
                                                    new ArrayTypeJson(), 
                                                    new URI("json:array"));
            graph.getTypeSystem().addPredefinedType(objectTypeHandle.getPersistent(), 
                                                    new ObjectTypeJson(), 
                                                    new URI("json:object"));
        }
        catch (Exception ex)
        {
            throw new RuntimeException();
        }
    }

    public HGHandle findType(URI typeId)
    {
        if (!typeId.getScheme().equals(getName()))
            return wrapped.findType(typeId);
        String typeName = typeId.getSchemeSpecificPart();
        if ("boolean".equals(typeName))
            return booleanTypeHandle;
        else if ("null".equals(typeName))
            return nullTypeHandle;
        else if ("string".equals(typeName))
            return stringTypeHandle;
        else if ("number".equals(typeName))
            return numberTypeHandle;
        else if ("array".equals(typeName))
            return arrayTypeHandle;
        else if ("object".equals(typeName))
            return objectTypeHandle;
        else // lookup URI->HGHandle index
            return graph.getTypeSystem().getHandleForIdentifier(typeId);
    }

    public void defineType(URI typeId, HGHandle typeHandle)
    {
        if (typeId.getScheme().equals(getName()))
            throw new UnsupportedOperationException();
        else
            wrapped.defineType(typeId, typeHandle);
    }

    public void removeType(URI typeId)
    {
        if (typeId.getScheme().equals(getName()))
            throw new UnsupportedOperationException();
        else
            wrapped.removeType(typeId);
    }

    public Object getTypeDescriptor(URI typeId)
    {
        if (typeId.getScheme().equals(getName()))
            throw new UnsupportedOperationException();
        else
            return wrapped.getTypeDescriptor(typeId);
    }

    public HGAtomType fromRuntimeType(HGHandle typeHandle, HGAtomType typeInstance)
    {
    	return wrapped.fromRuntimeType(typeHandle, typeInstance);
    }
    
    public HGAtomType toRuntimeType(HGHandle typeHandle, HGAtomType typeInstance)
    {
        return wrapped.toRuntimeType(typeHandle, typeInstance);
    }

    public URI toTypeURI(Object object)
    {
        if (! (object instanceof Json))
            return wrapped.toTypeURI(object);
        Json j = (Json)object;
        try
        {
            if (j.isArray())
                return new URI("json:array");
            else if (j.isObject())
                return new URI("json:object");
            else if (j.isNumber())
                return new URI("json:number");
            else if (j.isString())
                return new URI("json:string");
            else if (j.isBoolean())
                return new URI("json:boolean");
            else if (j.isNull())
                return new URI("json:null");
            else
                throw new IllegalArgumentException();
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }        
    }

    public URI toTypeURI(Class<?> javaClass)
    {
        return wrapped.toTypeURI(javaClass);
    }
    
    public static final HGHandle booleanTypeHandle = 
        UUIDHandleFactory.I.makeHandle("1182ce94-0d30-4a7a-84eb-308fc9dd6d53");
    public static final HGHandle numberTypeHandle = 
        UUIDHandleFactory.I.makeHandle("27caf688-e8bc-4532-bf43-7d0969b98325");
    public static final HGHandle nullTypeHandle = 
        UUIDHandleFactory.I.makeHandle("1fd98211-2412-4a9c-be33-11c07903064a");
    public static final HGHandle stringTypeHandle = 
        UUIDHandleFactory.I.makeHandle("c81e31fb-56a5-47bf-a117-d8586331f369");
    public static final HGHandle arrayTypeHandle = 
        UUIDHandleFactory.I.makeHandle("483e588c-e823-4dd7-b77a-80ffdf737e0d");
    public static final HGHandle objectTypeHandle = 
        UUIDHandleFactory.I.makeHandle("88308625-b6ea-4c5f-828c-cc9ffae25da4");
    
    /**
     * Return <code>true</code> if the passed in handle is one of the JSON HyperGraph types
     * @param typeHandle
     * @return
     */
    public static boolean isJsonType(HGHandle typeHandle)
    {
    	return objectTypeHandle.equals(typeHandle) ||
    		   arrayTypeHandle.equals(typeHandle) ||
    		   stringTypeHandle.equals(typeHandle) ||
    		   numberTypeHandle.equals(typeHandle) ||
    		   nullTypeHandle.equals(typeHandle) ||
    		   booleanTypeHandle.equals(typeHandle);
    }
}