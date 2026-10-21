package mjson.hgdb;

import mjson.Json;

/**
 * <p>
 * Thrown on an attempt to modify an immutable JSON value, that is any JSON element
 * that is not an entity. See <code>docs/storage-model-plan.md</code>.
 * </p>
 */
public class ImmutableJsonException extends JsonNodeException
{
    private static final long serialVersionUID = 1L;

    public ImmutableJsonException(String msg)
    {
        super(msg);
    }

    public ImmutableJsonException(String msg, Json data)
    {
        super(msg, data);
    }
}
