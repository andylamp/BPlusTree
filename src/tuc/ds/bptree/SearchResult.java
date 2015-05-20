package tuc.ds.bptree;

public class SearchResult {

    private TreeLeaf leafLoc;

    public int getIndex()
        {return index;}

    private int index;
    private boolean found;

    public SearchResult(TreeLeaf leaf, int index, boolean found) {
        this.leafLoc = leaf;
        this.index = index;
        this.found = found;
    }

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


}
