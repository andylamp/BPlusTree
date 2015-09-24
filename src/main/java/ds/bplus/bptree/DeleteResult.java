package ds.bplus.bptree;

import java.util.LinkedList;

/**
 *
 * Wrapper for deletions, stores the key as well as the
 * values deleted for that key (usually one or all).
 *
 */
public class DeleteResult {
    private final long key;
    private final boolean found;
    private final LinkedList<String> values;

    /**
     * Default constructor for single deletes
     *
     * @param key key that values are tied
     * @param value values deleted
     */
    public DeleteResult(long key, String value) {
        this.key = key;
        if(value != null) {
            values = new LinkedList<>();
            values.add(value);
            this.found = true;
        } else {
            this.values = null;
            this.found = false;
        }
    }

    /**
     * This is a more flexible constructor as we pass on
     * an already populated linked list.
     *
     * @param key key that values are tied
     * @param values already populated list of deleted values
     */
    public DeleteResult(long key, LinkedList<String> values) {
        this.key = key;
        this.values = values;
        this.found = !(values == null || values.isEmpty());
    }

    // -- Getters --

    public LinkedList<String> getValues()
        {return(values);}

    public long getKey()
        {return(key);}

    public boolean isFound()
        {return(found);}
}
