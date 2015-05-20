package tuc.ds.bptree;

/**
 * Wrapper to result the search results with ease, since we
 * need to store multiple information in our results which are:
 *
 * -- the leaf that the (K, V) might reside
 * -- the index where is the key that is less or matches our key
 * -- finally a convenient boolean flag to indicate the search result (T/F)
 *
 */

public class SearchResult {

    private TreeLeaf leafLoc;       // the leaf which our (K, V) might reside
    private int index;              // index where first key is <= our requested key
    private boolean found;          // we found the requested key?

    /**
     * We only have one constructor and feed it all the above information
     *
     * @param leaf the leaf which our (K, V) might reside
     * @param index index where first key is <= our requested key
     * @param found we found the requested key?
     */
    public SearchResult(TreeLeaf leaf, int index, boolean found) {
        this.leafLoc = leaf;
        this.index = index;
        this.found = found;
    }


    // -- Just Setters and Getters


    public TreeLeaf getLeaf()
        {return(this.leafLoc);}

    public void setLeaf(TreeLeaf leaf)
        {this.leafLoc = leaf;}

    public String getValue()
        {return(leafLoc.getValueAt(index));}

    public long getKey()
        {return(leafLoc.getKeyAt(index));}

    public boolean isFound()
        {return(found);}

    public int getIndex()
        {return index;}


}
