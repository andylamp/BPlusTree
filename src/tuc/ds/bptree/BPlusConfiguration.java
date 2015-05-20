package tuc.ds.bptree;

public class BPlusConfiguration {

    private int pageSize;
    private int keySize;
    private int entrySize;
    private int treeDegree;
    private int headerSize;
    private int leafHeaderSize;
    private int internalNodeHeadersize;
    private int leafNodeDegree;
    private int lookupPageSize;
    private String pagePrefix;

    /**
     *
     *  Default constructor which initializes all
     *  settings to the predefined defaults.
     *
     */
    public BPlusConfiguration() {
        this.pageSize = 128;                           // page size (in bytes)
        this.entrySize = 10;                            // each entry size (in bytes)
        this.keySize = 8;                               // key size (in bytes)
        this.headerSize =                               // header size in bytes
                Integer.SIZE * 5;
        this.internalNodeHeadersize = 6;
        this.leafHeaderSize = 22;
        this.pagePrefix = "vp_";                        // page file prefix
        this.lookupPageSize = pageSize - headerSize;    // lookup page size
        // now calculate the tree degree
        this.treeDegree = calculateDegree(2*keySize, internalNodeHeadersize);
        this.leafNodeDegree = calculateDegree(keySize+entrySize, leafHeaderSize);
    }

    /**
     * Overloaded constructor
     *
     * @param pageSize page size (default is 1024 bytes)
     * @param keySize key size (default is long [8 bytes])
     * @param entrySize satellite data (default is 10 bytes)
     * @param pagePrefix file prefix (default vp_)
     */
    public BPlusConfiguration(int pageSize, int keySize,
                              int entrySize,// int lookupPageSize,
                              String pagePrefix) {
        this.pageSize = pageSize;                           // page size (in bytes)
        this.entrySize = entrySize;                         // entry size (in bytes)
        this.pagePrefix = pagePrefix;                       // page file prefix
        this.headerSize =                                   // header size in bytes
                Integer.SIZE * 4 + 2 * Long.SIZE;
        this.internalNodeHeadersize = 6;
        this.leafHeaderSize = 22;
        this.lookupPageSize = lookupPageSize - headerSize;  // lookup page size
        // now calculate the tree degree
        this.treeDegree = calculateDegree(2*keySize, internalNodeHeadersize);
        this.leafNodeDegree = calculateDegree(keySize+entrySize, leafHeaderSize);
    }

    private int calculateDegree(int elementSize, int elementHeaderSize)
        {return((pageSize-elementHeaderSize)/(2*elementSize));}

    public int getPageSize()
        {return pageSize;}

    public int getEntrySize()
        {return entrySize;}

    public int getTreeDegree()
        {return treeDegree;}

    public int getMaxInternalNodeCapacity()
        {return((2*treeDegree) - 1);}

    public int getMaxLeafNodeCapacity()
        {return((2*leafNodeDegree) - 1);}

    public int getMinLeafNodeCapacity()
        {return(leafNodeDegree-1);}

    public int getMinInternalNodeCapacity()
        {return(treeDegree-1);}

    public String getPagePrefix()
        {return pagePrefix;}

    public int getKeySize()
        {return keySize;}

    public int getLeafNodeDegree()
        {return leafNodeDegree;}

    public int getLookupPageDegree()
        {return(pageSize/keySize);}

    public int getLookupPageSize()
        {return(lookupPageSize);}

    public int getHeaderSize()
        {return(headerSize);}

    public int getPageCountOffset()
        {return(headerSize-8);}
}