package ds.bplus.bptree;

import java.util.LinkedList;

/**
 * Wrapper to result the search results with ease, since we
 * need to store multiple information in our results which are:
 *
 * -- the leaf that the (K, V) might reside
 * -- the index where is the key that is less or matches our key
 * -- finally a convenient boolean flag to indicate the search result (T/F)
 *
 */
@SuppressWarnings({"WeakerAccess", "unused"})
public class SearchResult {

    private final int index;                // index where first key is <= our requested key
    private final boolean found;            // we found the requested key?
    private TreeLeaf leafLoc;               // the leaf which our (K, V) might reside
    private LinkedList<String> ovfValues;   // linked list in the case of non-unique queries

    /**
     * Constructor for unique queries, hence feed it all the above information
     *
     * @param leaf the leaf which our (K, V) might reside
     * @param index index where first key is <= our requested key
     * @param found we found the requested key?
     */
    public SearchResult(TreeLeaf leaf, int index, boolean found) {
        this.leafLoc = leaf;
        this.index = index;
        this.found = found;
        // add it only if we found it.
        if(found) {
            ovfValues = new LinkedList<>();
            ovfValues.push((leafLoc.getValueAt(index)));
        }
    }

    /**
     * Constructor for returning all duplicates, we assume that the
     * list has already been populated inside the tree.
     *
     * @param leaf the leaf which our (K, V) might reside
     * @param index index where the first key is equal with our requested key
     * @param vals the linked list with the values
     */
    public SearchResult(TreeLeaf leaf, int index, LinkedList<String> vals) {
        this.leafLoc = leaf;
        this.index = index;
        this.found = true;
        this.ovfValues = vals;
    }

    // -- Just Setters and Getters

    public TreeLeaf getLeaf()
        {return(this.leafLoc);}

    public void setLeaf(TreeLeaf leaf)
        {this.leafLoc = leaf;}

    public LinkedList<String> getValues()
        {return(ovfValues);}

    public long getKey()
        {return(leafLoc.getKeyAt(index));}

    public boolean isFound()
        {return(found);}

    public int getIndex()
        {return index;}


}
