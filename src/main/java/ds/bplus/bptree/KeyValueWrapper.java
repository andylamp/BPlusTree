package ds.bplus.bptree;

/**
 * Wrapper to conveniently return the (Key, Value) pair
 * without having to resort to "weird" solutions.
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class KeyValueWrapper {

    private final long key;           // key
    private final String value;       // value

    /**
     * This is the only constructor... as we only
     * need to set them
     * @param key the key of (K, V) pair
     * @param value the value of the (K, V) pair
     */
    public KeyValueWrapper(long key, String value) {
        this.key = key;
        this.value = value;
    }

    public long getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }


}
