package tuc.ds.bptree;

import sun.plugin.dom.exception.InvalidStateException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.InvalidPropertiesFormatException;
import java.util.LinkedList;

public class BPlusTree {

    private TreeNode root;
    private TreeNode leftChild;
    private RandomAccessFile treeFile;
    private BPlusConfiguration conf;
    private LinkedList<Long> freeSlotPool;
    private long totalTreePages;
    private BPlusTreePerformanceCounter bPerf = null;

    /**
     * Super basic constructor, create everything using their
     * default values...
     *
     * @throws IOException
     */
    public BPlusTree() throws IOException {
        this.conf = new BPlusConfiguration();
        this.totalTreePages = 0L;
        this.freeSlotPool = new LinkedList<>();
        bPerf = new BPlusTreePerformanceCounter(false);
        bPerf.setBTree(this);
        openFile("tree.bin", "rw+", conf);
    }

    /**
     * Basic constructor that creates a B+ Tree instance based
     * on an already generated configuration.
     *
     * Default I/O mode is to *truncate* file
     *
     * @param conf B+ Tree configuration instance
     * @param bPerf performance counter class
     * @throws IOException
     */
    public BPlusTree(BPlusConfiguration conf, BPlusTreePerformanceCounter bPerf)
            throws IOException {
        this.conf = conf;
        this.totalTreePages = 0L;
        this.freeSlotPool = new LinkedList<>();
        this.bPerf = bPerf;
        bPerf.setBTree(this);
        openFile("tree.bin", "rw+", conf);
    }

    /**
     *
     * This constructor allows us to create a B+ tree as before
     * given our configuration but allows us to tweak the I/O flags
     *
     *
     * @param conf B+ Tree configuration instance
     * @param mode mode of file opening
     * @param bPerf performance counter class
     * @throws IOException
     */
    public BPlusTree(BPlusConfiguration conf, String mode,
                     BPlusTreePerformanceCounter bPerf)
            throws IOException {
        this.conf = conf;
        this.totalTreePages = 0L;
        this.freeSlotPool = new LinkedList<>();
        this.bPerf = bPerf;
        bPerf.setBTree(this);
        openFile("tree.bin", mode, conf);
    }

    /**
     * This constructor allows the most customization as it
     * allows us to set the I/O flags as well as the file name
     * of the resulting file.
     *
     * @param conf B+ Tree configuration instance
     * @param mode I/O mode
     * @param treeFilePath file path for the file
     * @param bPerf performance counter class
     * @throws IOException
     */
    public BPlusTree(BPlusConfiguration conf, String mode,
                     String treeFilePath, BPlusTreePerformanceCounter bPerf)
            throws IOException {
        this.conf = conf;
        this.freeSlotPool = new LinkedList<>();
        this.totalTreePages = 0L;
        this.bPerf = bPerf;
        bPerf.setBTree(this);
        openFile(treeFilePath, mode, conf);
    }

    /**
     * Insert the key into the tree while also providing the flexibility
     * of having unique keys or not at will.
     *
     * @param key key to add
     * @param value value of the key
     * @param unique allow duplicates for this run?
     * @throws IOException
     */
    public void insertKey(long key, String value, boolean unique) throws IOException {

        if(root == null)
            {throw new IllegalStateException("Can't insert to null tree");}

        if(key < 0)
            {throw new NumberFormatException("Can't have negative keys, sorry.");}

        // check if our root is full
        if(root.isFull(conf)) {
            // allocate a new *internal* node
            leftChild = this.root;
            TreeInternalNode tbuf = new TreeInternalNode(TreeNodeType.TREE_ROOT_INTERNAL,
                    generateFirstAvailablePageIndex(conf));
            tbuf.addPointerAt(0, leftChild.getPageIndex());
            this.root = tbuf;

            // split root.
            splitTreeNode(tbuf, 0);
            insertNonFull(tbuf, key, value, unique);
        }
        else
            {insertNonFull(root, key, value, unique);}
        bPerf.incrementTotalInsertions();
    }

    /**
     *
     * This function is based on the similar function prototype that
     * is given by CLRS for B-Tree but is altered (quite a bit) to
     * be able to be used for B+ Trees.
     *
     * The main difference is that when the split happens *all* keys
     * are preserved and the first key of the right node is moved up.
     *
     * For example say we have the following (order is assumed to be 3):
     *
     *          [ k1 k2 k3 k4 ]
     *
     * This split would result in the following:
     *
     *              [ k3 ]
     *              /   \
     *            /      \
     *          /         \
     *     [ k1 k2 ]   [ k3 k4 ]
     *
     * This function requires at least *3* page writes plus the commit of
     * the updated page count to the file header. In the case that after
     * splitting we have a new root we must commit the new root index
     * to the file header as well; this happens transparently inside
     * writeNode method and is not explicitly done here.
     *
     * @param n internal node "parenting" the split
     * @param index index in the node n that we need to add the median
     */
    private void splitTreeNode(TreeInternalNode n, int index) throws IOException {

//        System.out.println("-- Splitting node with index: " +
//                leftChild.getPageIndex() + " of type: " +
//                leftChild.getNodeType().toString());

        int setIndex;
        TreeNode znode;
        long keyToAdd;
        TreeNode ynode = leftChild; // x.c_{i}
        if(ynode.isInternalNode()) {
            znode = new TreeInternalNode(TreeNodeType.TREE_INTERNAL_NODE,
                    generateFirstAvailablePageIndex(conf));

            bPerf.incrementTotalInternalNodes();


            setIndex =  conf.getTreeDegree()-1;

            int i;
            for(i = 0; i < setIndex; i++) {
                znode.addToKeyArrayAt(i, ynode.popKey());
                ((TreeInternalNode)znode).addPointerAt(i,
                        ((TreeInternalNode)ynode).popPointer());
            }
            ((TreeInternalNode)znode).addPointerAt(i,
                    ((TreeInternalNode)ynode).popPointer());
            keyToAdd = ynode.popKey();

            znode.setCurrentCapacity(setIndex);
            ynode.setCurrentCapacity(setIndex);

            // it it was the root, invalidate it and make it a regular internal node
            if(ynode.isRoot()) {
                ynode.setNodeType(TreeNodeType.TREE_INTERNAL_NODE);
                bPerf.incrementRootSplits();
            }
            bPerf.incrementInternalNodeSplits();

            // update pointer at n_{index+1}
            n.addPointerAt(index, znode.getPageIndex());
            // update key value at n[index]
            n.addToKeyArrayAt(index, keyToAdd);
            // adjust capacity
            n.incrementCapacity();
        }
        // we have a leaf
        else {
            znode = new TreeLeaf(((TreeLeaf)ynode).getNextPagePointer(),
                    ynode.getPageIndex(), TreeNodeType.TREE_LEAF,
                    generateFirstAvailablePageIndex(conf));

            bPerf.incrementTotalLeaves();

            // update pointers in ynode, only have to update next pointer
            ((TreeLeaf)ynode).setNextPagePointer(znode.getPageIndex());

            setIndex = conf.getLeafNodeDegree()-1;

            for(int i = 0; i < setIndex; i++) {
                znode.pushToKeyArray(ynode.removeLastKey());
                ((TreeLeaf)znode).pushToValueList(((TreeLeaf)ynode).removeLastValue());
                ((TreeLeaf)znode).pushToOverflowList(
                        ((TreeLeaf) ynode).removeLastOverflowPointer());
                znode.incrementCapacity();
                ynode.decrementCapacity();
            }

            // it it was the root, invalidate it and make it a regular leaf
            if(ynode.isRoot()) {
                ynode.setNodeType(TreeNodeType.TREE_LEAF);
                bPerf.incrementRootSplits();
            }
            bPerf.incrementTotalLeafSplits();

            // update pointer at n_{index+1}
            n.addPointerAt(index + 1, znode.getPageIndex());
            // update key value at n[index]
            n.addToKeyArrayAt(index, znode.getKeyAt(0));
            // adjust capacity
            n.incrementCapacity();
        }

        // commit the changes
        znode.writeNode(treeFile, conf, bPerf);
        ynode.writeNode(treeFile, conf, bPerf);
        n.writeNode(treeFile, conf, bPerf);
        // commit page counts
        treeFile.seek(conf.getPageCountOffset());
        treeFile.writeLong(totalTreePages);

    }

    /**
     * This function is responsible handling the creation of overflow pages. We have
     * generally two distinct cases which are the following:
     *
     *  * Create an overflow page directly from a B+ TreeLeaf.
     *  * Add an overflow page directly after an existing one.
     *
     *  In both cases for convenience we update all the required metrics as well as
     *  push to the newly created page the required value.
     *
     * @param n node to add the page
     * @param index this is only used in the case of a leaf
     * @param value value to push in the new page
     * @return the new page reference
     * @throws IOException
     */
    private TreeOverflow createOverflowPage(TreeNode n, int index, String value)
            throws IOException {
        TreeOverflow novf;
        if(n.isOverflow()) {
            TreeOverflow ovf = (TreeOverflow)n;
            novf = new TreeOverflow(-1L, ovf.getPageIndex(),
                    generateFirstAvailablePageIndex(conf));
            // push the first value
            novf.pushToValueList(value);
            novf.incrementCapacity();
            // update overflow pointer to parent node
            ovf.setNextPagePointer(novf.getPageIndex());
            // commit changes to new overflow page
            novf.writeNode(treeFile, conf, bPerf);
            // commit changes to old overflow page
            ovf.writeNode(treeFile, conf, bPerf);
        } else if(n.isLeaf()) {
            TreeLeaf l = (TreeLeaf)n;
            novf = new TreeOverflow(-1L, l.getPageIndex(),
                    generateFirstAvailablePageIndex(conf));
            // push the first value
            novf.pushToValueList(value);
            novf.incrementCapacity();
            // update overflow pointer to parent node
            l.addToOverflowList(index, novf.getPageIndex());
            // commit changes to overflow page
            novf.writeNode(treeFile, conf, bPerf);
            // commit changes to leaf page
            l.writeNode(treeFile, conf, bPerf);
            // commit page counts
        } else {
            throw new InvalidStateException("Expected Leaf or Overflow, got instead: "
                    + n.getNodeType().toString());
        }

        bPerf.incrementTotalOverflowPages();
        // commit page counts
        treeFile.seek(conf.getPageCountOffset());
        treeFile.writeLong(totalTreePages);
        // finally return the page
        return(novf);
    }

    /**
     * This function is inspired but the one given in CLRS for inserting a key to
     * a B-Tree but as splitTreeNode has been (heavily) modified in order to be used
     * in our B+ Tree. It supports handling duplicate keys (if enabled) as well.
     *
     * It is able to insert the (Key, Value) paris using only one pass through the tree.
     *
     * @param n current node
     * @param key key to add
     * @param value value paired with the key
     * @param unique allow duplicate entries for this time?
     * @throws IOException
     */
    private void insertNonFull(TreeNode n, long key, String value, boolean unique)
            throws IOException {
        boolean goLeft = true;
        int i = n.getCurrentCapacity()-1;
        // descend down the node
        while(i >= 0 && key < n.getKeyAt(i)) {i--;}
        // correction
        i++;
        // check if we have a leaf
        if(n.isLeaf()) {
            TreeLeaf l = (TreeLeaf)n;

            // before we add it, let's check if the key already exists
            // and if it does pull up (or create) the overflow page and
            // add the value there.
            //
            // Not that we do *not* add the key if we have a true unique flag
            if(n.getCurrentCapacity() > 0 && n.getKeyAt((i == 0 ? i : i-1)) == key) {

                if(unique) {
                    //System.out.println("Duplicate entry found and unique " +
                    //        "flag enabled, can't add");
                    return;
                }

                //System.out.println("Duplicate found! Adding to overflow page!");

                // overflow page does not exist, yet; time to create it!
                if(l.getOverflowPointerAt(i-1) < 0)
                    {createOverflowPage(l, (i-1), value);}
                // page already exists, so pull it and check if it has
                // available space, if it does all is good; otherwise we
                // pull the next overflow page or we create another one.
                else {
                    TreeOverflow ovf = (TreeOverflow)readNode(l.getOverflowPointerAt(i-1));

                    while(ovf.isFull(conf)) {
                        // check if we have more, if not create
                        if(ovf.getNextPagePointer() < 0)
                            // create page and return
                            {createOverflowPage(ovf, -1, value); return;}
                        // load the next page
                        else
                            {ovf = (TreeOverflow)readNode(ovf.getNextPagePointer());}
                    }

                    // if the loaded page is not full then add it.
                    ovf.pushToValueList(value);
                    ovf.incrementCapacity();
                    ovf.writeNode(treeFile, conf, bPerf);
                }
            }
            // we have a new key insert
            else {
                // now add the (Key, Value) pair
                l.addToValueList(i, value);
                l.addToKeyArrayAt(i, key);
                // also create a NULL overflow pointer
                l.addToOverflowList(i, -1L);
                l.incrementCapacity();

                // commit the changes
                l.writeNode(treeFile, conf, bPerf);
            }

        } else {
            TreeInternalNode inode = (TreeInternalNode)n;
            leftChild = readNode(inode.getPointerAt(i));

            TreeNode tmpRight = null;
            if(leftChild.isFull(conf)) {
                splitTreeNode(inode, i);
                if (key > n.getKeyAt(i)) {
                    goLeft = false;
                    tmpRight = readNode(inode.getPointerAt(i+1));
                }
            }

            insertNonFull(goLeft ? leftChild : tmpRight, key, value, unique);
        }
    }

    /**
     * Function to parse the overflow pages specifically for the range queries
     *
     * @param l leaf which contains the key with the overflow page
     * @param index index of the key
     * @param res where to store the results
     * @throws IOException
     */
    private void parseOverflowPages(TreeLeaf l, int index, RangeResult res)
            throws IOException {
        TreeOverflow ovfPage = (TreeOverflow)readNode(l.getOverflowPointerAt(index));
        int icap = 0;
        while(icap < ovfPage.getCurrentCapacity()) {
            res.getQueryResult().add(new KeyValueWrapper(l.getKeyAt(index),
                    ovfPage.getValueAt(icap)));
            icap++;
            // check if we have more pages
            if(icap == ovfPage.getCurrentCapacity() &&
                    ovfPage.getNextPagePointer() != -1L) {
                ovfPage = (TreeOverflow)readNode(ovfPage.getNextPagePointer());
                icap = 0;
            }
        }
    }

    /**
     * Handle range search queries with a bit of twist on how we handle duplicate keys.
     *
     *  We have two basic cases depending duplicate keys, which is basically whether
     *  we actually return them or not.
     *
     * @param minKey min key of the range
     * @param maxKey max key of the range
     * @param unique return only *first* encounter of the (Key, Value) pairs or all?
     * @return the results packed in a neat class for handling
     * @throws IOException
     */
    public RangeResult rangeSearch(long minKey, long maxKey, boolean unique)
            throws IOException {
        SearchResult sMin = searchKey(minKey, unique);
        SearchResult sMax;
        RangeResult rangeQueryResult = new RangeResult();
        if(sMin.isFound()) {
            // read up until we find a key that's greater than maxKey
            // or the last entry.

            int i = sMin.getIndex();
            while(sMin.getLeaf().getKeyAt(i) <= maxKey) {
                rangeQueryResult.getQueryResult().
                        add(new KeyValueWrapper(sMin.getLeaf().getKeyAt(i),
                                sMin.getLeaf().getValueAt(i)));

                // check if we have an overflow page
                if(!unique && sMin.getLeaf().getOverflowPointerAt(i) != -1)
                    {parseOverflowPages(sMin.getLeaf(), i, rangeQueryResult);}

                i++;

                // check if we need to read the next block
                if(i == sMin.getLeaf().getCurrentCapacity()) {
                    // check if we have a next node to load.
                    if(sMin.getLeaf().getNextPagePointer() < 0)
                        // if not just break the loop
                        {break;}
                    sMin.setLeaf((TreeLeaf)readNode(sMin.getLeaf().getNextPagePointer()));
                    i = 0;
                }
            }

        }
        // this is the case where both searches might fail to find something, but
        // we *might* have something between in the given range. To account for
        // that even if we have *not* found something we will return those results
        // instead. For example say we have a range of [2, 5] and we only have keys
        // from [3, 4], thus both searches for min and max would fail to find a
        // matching key in both cases. Thing is to account for that *both* results
        // will be stopped at the first key that is less than min and max values
        // given even if we did not find anything.
        else {
            sMax = searchKey(maxKey, unique);
            int i = sMax.getIndex();
            while(i >= 0 && sMax.getLeaf().getKeyAt(i) >= minKey) {
                rangeQueryResult.getQueryResult().
                        add(new KeyValueWrapper(sMax.getLeaf().getKeyAt(i),
                                sMax.getLeaf().getValueAt(i)));

                // check if we have an overflow page
                if(!unique && sMax.getLeaf().getOverflowPointerAt(i) != -1)
                    {parseOverflowPages(sMax.getLeaf(), i, rangeQueryResult);}

                i--;
                // check if we need to read the next block
                if(i < 0) {
                    // check if we do have another node to load
                    if(sMax.getLeaf().getPrevPagePointer() < 0)
                    // if not just break the loop
                        {break;}
                    sMax.setLeaf((TreeLeaf)readNode(sMax.getLeaf().getPrevPagePointer()));
                    // set it to max length
                    i = sMax.getLeaf().getCurrentCapacity()-1;
                }
            }

        }
        bPerf.incrementTotalRangeQueries();
        // finally return the result list (empty or not)
        return(rangeQueryResult);
    }

    /**
     * Search inside the B+ Tree data structure for the requested key; based on the
     * unique flag we have two choices which are the following:
     *
     *  * unique flag true: return the *first* value that matches the given key
     *  * unique flag false: return *all* the values that match the given key
     *
     *  The second one including a couple more page accesses as if the key has any
     *  duplicates they will be stored in one or multiple overflow pages depending
     *  on the number of duplicates. Hence we have to pay for those disk accesses
     *  as well.
     *
     * @param key key to match
     * @param unique return *all* matching (Key, Value) pairs or the *first* found
     * @return the search result
     * @throws IOException
     */
    public SearchResult searchKey(long key, boolean unique)
            throws IOException
        {bPerf.incrementTotalSearches(); return(searchKey(this.root, key, unique));}


    private SearchResult searchKey(TreeNode node, long key, boolean unique)
            throws IOException {

        int i = 0;

        while(i < node.getCurrentCapacity() && key >= node.getKeyAt(i)) {i++;}

        // check if we found it
        if(node.isLeaf()) {
            i--;
            //TreeLeaf leaf = (TreeLeaf) readNode(((TreeLeaf)node).getNextPagePointer());

            //TreeLeaf leaf1 = (TreeLeaf) readNode(((TreeLeaf)leaf).getNextPagePointer());
            if(i >= 0 && i < node.getCurrentCapacity() && key == node.getKeyAt(i)) {

                // we found the key, depending on the unique flag handle accordingly
                if(unique || ((TreeLeaf)node).getOverflowPointerAt(i) == -1L )
                    {return(new SearchResult((TreeLeaf)node, i, true));}
                // handle the case of duplicates where actual overflow pages exist
                else {
                    TreeLeaf lbuf = (TreeLeaf)node;
                    TreeOverflow ovfBuf = (TreeOverflow)readNode(lbuf.getOverflowPointerAt(i));
                    LinkedList<String> ovfList = new LinkedList<>();
                    // add the current one
                    ovfList.add(lbuf.getValueAt(i));
                    int icap = 0;
                    // loop through all the overflow pages
                    while(icap < ovfBuf.getCurrentCapacity()) {
                        ovfList.add(ovfBuf.getValueAt(icap));
                        icap++;
                        // advance if we have another page
                        if(icap == ovfBuf.getCurrentCapacity() &&
                                ovfBuf.getNextPagePointer() != -1L) {
                            ovfBuf = (TreeOverflow)readNode(ovfBuf.getNextPagePointer());
                            icap = 0;
                        }
                    }
                    // now after populating the list return the search result
                    return(new SearchResult((TreeLeaf)node, i, ovfList));
                }
            }
            else
                // we found nothing, use the unique constructor anyway.
                {return(new SearchResult((TreeLeaf)node, i, false));}

        }
        // probably it's an internal node, descend to a leaf
        else {
            TreeNode t = readNode(((TreeInternalNode)node).getPointerAt(i));
            return(searchKey(t, key, unique));
        }

    }

    public int deleteKey(long key, boolean unique) {
        return(traverseAndDelete(root, key, unique));
    }

    private int traverseAndDelete(TreeNode parent, long key,
                                  boolean unique) {
        return(0);
    }

    private TreeNode createTree() throws IOException {
        if(root == null) {
            root = new TreeLeaf(-1, -1,
                    TreeNodeType.TREE_ROOT_LEAF,
                    generateFirstAvailablePageIndex(conf));
            bPerf.incrementTotalPages();
            bPerf.incrementTotalLeaves();
            // write the file
            root.writeNode(treeFile, conf, bPerf);
        }
        return(root);
    }


    /**
     * Map the short value to an actual node type enumeration value.
     * This paradoxically is the opposite of that we do in the similarly named
     * function in each node.
     *
     * @param pval a value read from the file indicating which type of node this is
     * @return nodeType equivalent
     * @throws InvalidPropertiesFormatException
     */
    private TreeNodeType getPageType(short pval)
            throws InvalidPropertiesFormatException {
        switch(pval) {

            case 1:         // LEAF
                {return(TreeNodeType.TREE_LEAF);}

            case 2:         // INTERNAL NODE
                {return(TreeNodeType.TREE_INTERNAL_NODE);}

            case 3:         // INTERNAL NODE /w ROOT
                {return(TreeNodeType.TREE_ROOT_INTERNAL);}

            case 4:         // LEAF NODE /w ROOT
                {return(TreeNodeType.TREE_ROOT_LEAF);}

            case 5:         // LEAF OVERFLOW NODE
                {return(TreeNodeType.TREE_LEAF_OVERFLOW);}

            default: {
                throw new InvalidPropertiesFormatException("Unknown " +
                        "node value read; file possibly corrupt?");
            }
        }
    }

    /**
     * Calculate the page offset taking in account the
     * for the lookup page at the start of the file.
     *
     * @param index index of the page
     * @return the calculated file offset to be fed in seek.
     */
    private long calculatePageOffset(long index)
        {return(conf.getPageSize()*(index+1));}

    /**
     * Read each tree node and return it as a generic type
     *
     * @param index index of the page in the file
     * @return a TreeNode object referencing to the loaded page
     * @throws IOException
     */
    private TreeNode readNode(long index) throws IOException {
        //calculatePageOffset(index)
        treeFile.seek(index);
        // get the page type
        TreeNodeType nt = getPageType(treeFile.readShort());

        // handle internal node reading
        if(isInternalNode(nt)) {
            TreeInternalNode tnode = new TreeInternalNode(nt, index);
            int curCap = treeFile.readInt();
            for(int i = 0; i < curCap; i++) {
                tnode.addToKeyArrayAt(i, treeFile.readLong());
                tnode.addPointerAt(i, treeFile.readLong());
            }
            // add the final pointer
            tnode.addPointerAt(curCap, treeFile.readLong());
            // update the capacity
            tnode.setCurrentCapacity(curCap);
            bPerf.incrementTotalInternalNodeReads();
            return(tnode);
        }
        // check if we have an overflow page
        else if(isOverflowPage(nt)) {
            long nextptr = treeFile.readLong();
            long prevptr = treeFile.readLong();
            int curCap = treeFile.readInt();
            byte[] strBuf = new byte[conf.getEntrySize()];
            TreeOverflow tnode = new TreeOverflow(nextptr, prevptr, index);

            // read entries
            for(int i = 0; i < curCap; i++) {
                treeFile.read(strBuf);
                tnode.addToValueList(i, new String(strBuf));
            }
            // update capacity
            tnode.setCurrentCapacity(curCap);
            bPerf.incrementTotalOverflowReads();
            return(tnode);
        }
        // well, it must be a leaf node
        else {
            long nextptr = treeFile.readLong();
            long prevptr = treeFile.readLong();
            int curCap = treeFile.readInt();
            byte[] strBuf = new byte[conf.getEntrySize()];
            TreeLeaf tnode = new TreeLeaf(nextptr, prevptr, nt, index);

            // read entries
            for(int i = 0; i < curCap; i++) {
                tnode.addToKeyArrayAt(i, treeFile.readLong());
                tnode.addToOverflowList(i, treeFile.readLong());
                treeFile.read(strBuf);
                tnode.addToValueList(i, new String(strBuf));
            }
            // update capacity
            tnode.setCurrentCapacity(curCap);
            bPerf.incrementTotalLeafNodeReads();
            return(tnode);
        }
    }

    /**
     * Check if the node is an internal node
     *
     * @param nt nodeType of the node we want to check
     * @return return true if it's an Internal Node, false if it's not.
     */
    private boolean isInternalNode(TreeNodeType nt) {
        return(nt == TreeNodeType.TREE_INTERNAL_NODE ||
                nt == TreeNodeType.TREE_ROOT_INTERNAL);
    }

    /**
     * Check if the node is an overflow page
     *
     * @param nt nodeType of the node we want to check
     * @return return true if it's an overflow page, false if it's not.
     */
    public boolean isOverflowPage(TreeNodeType nt)
        {return(nt == TreeNodeType.TREE_LEAF_OVERFLOW);}

    /*
    public boolean isLeaf(TreeNodeType nt) {
        return(nt == TreeNodeType.TREE_LEAF ||
                nt == TreeNodeType.TREE_LEAF_OVERFLOW ||
                nt == TreeNodeType.TREE_ROOT_LEAF);
    }
    */

    /**
     * Reads an existing file and generates a B+ configuration based on the stored values
     *
     * @param r file to read from
     * @param generateConf generate configuration?
     * @return new configuration based on read values (if enabled) or null
     * @throws IOException
     */
    private BPlusConfiguration readFileHeader(RandomAccessFile r, boolean generateConf)
            throws IOException {
        r.seek(0L);

        // read the header number
        int headerNumber = r.readInt();

        if(headerNumber < 0)
            {throw new InvalidStateException("Negative header number found...");}

        // read the page size
        int pageSize = r.readInt();

        if(pageSize < 0)
            {throw new InvalidStateException("Cannot create a tree with negative page size");}

        // read the entry size
        int entrySize = r.readInt();

        if(entrySize <= 0)
            {throw new InvalidStateException("Entry size must be > 0");}

        // key size
        int keySize = r.readInt();

        if(keySize > 8 || keySize < 4)
            {throw new InvalidStateException("Key size but be either 4 or 8 bytes");}

        // read the number of pages (excluding the lookup)
        totalTreePages = r.readLong();

        if(totalTreePages < 0)
            {throw new InvalidStateException("Tree page number cannot be < 0");}

        // read the root index
        long rootIndex = r.readLong();

        if(rootIndex < 0)
            {throw new InvalidStateException("Root can't have index < 0");}

        // read the root.
        root = readNode(rootIndex);

        // finally if needed create a configuration file
        if(generateConf)
            {return(new BPlusConfiguration(pageSize, keySize, entrySize));}
        else
            {return(null);}
    }


    /**
     * Writes the file header containing all the juicy details
     *
     * @param conf valid configuration
     * @throws IOException
     */
    private void writeFileHeader(BPlusConfiguration conf)
            throws IOException {
        treeFile.seek(0L);
        treeFile.writeInt(conf.getHeaderSize());
        treeFile.writeInt(conf.getPageSize());
        treeFile.writeInt(conf.getEntrySize());
        treeFile.writeInt(conf.getKeySize());
        treeFile.writeLong(totalTreePages);
        treeFile.writeLong(root.getPageIndex());
    }

    /**
     * Opens a file descriptor to our B+ Tree storage file; it can
     * handle already existing files as well without recreating them
     * unless explcitly stated.
     *
     * @param path file path
     * @param mode mode of opening (basically to truncate it or not)
     * @param opt configuration reference
     * @throws IOException
     */
    private void openFile(String path, String mode, BPlusConfiguration opt)
            throws IOException {
        File f = new File(path);
        String stmode = mode.substring(0, 2);
        treeFile = new RandomAccessFile(path, stmode);
        // check if the file already exists
        if(f.exists() && !mode.contains("+")) {
            System.out.println("File already exists (size: " + treeFile.length() +
                    " bytes), trying to read it...");
            // read the header
            conf = readFileHeader(treeFile, true);
            // read the lookup page
            initializeLookupPage(f.exists());
            System.out.println("File seems to be valid. Loaded OK!");
        }
        // if we have to start anew, do so.
        else {
            System.out.println("Initializing the file...");
            System.out.println("Tracking I/O performance as well");
            treeFile.setLength(0);
            conf = opt == null ? new BPlusConfiguration() : opt;
            initializeLookupPage(false);
            createTree();
            writeFileHeader(conf);
            System.out.println("Done!");
        }
    }

    /**
     * Just commit the tree by actually closing the FD
     * thus flushing the buffers.
     *
     * @throws IOException
     */
    public void commitTree() throws IOException
        {this.treeFile.close();}

    /**
     * This function initializes the look-up page; in the simple
     * case that it does not already exist it just creates an
     * empty page by witting -1L all over it. If it exists then
     * we load it into memory for further use.
     *
     * @param exists flag to indicate if the file already exists
     * @throws IOException
     */
    private void initializeLookupPage(boolean exists) throws IOException {
        // get to the beginning of the file after the header
        this.treeFile.seek(conf.getHeaderSize());

        // check if already have a page, if not create it
        if(!exists) {
            for(int i = 0; i < conf.getLookupPageDegree(); i++)
                {this.treeFile.writeLong(-1);}
        }
        // if we do, read it.
        else {
            long val;
            for(int i = 0; i < conf.getLookupPageDegree(); i++) {
                if((val = this.treeFile.readLong()) == -1) {break;}
                this.freeSlotPool.add(val);
            }
        }
    }

    /**
     * Generate the first available index for a page.
     *
     * @param conf B+ configuration reference
     * @return page index
     */
    private long generateFirstAvailablePageIndex(BPlusConfiguration conf) {
        long index;
        // check if we have used pages
        if(freeSlotPool.size() > 0)
            {index = freeSlotPool.pop(); totalTreePages++; return(index);}
        // if not pad to the end of the file.
        else
            {index = conf.getPageSize() * (totalTreePages + 1); totalTreePages++; return(index);}
    }

    /**
     * Return the current configuration
     *
     * @return the configuration reference
     */
    public BPlusConfiguration getTreeConfiguration()
        {return(conf);}

    /**
     * Prints the current configuration to stdout.
     */
    public void printCurrentConfiguration()
        {conf.printConfiguration();}

    public void printTree() throws IOException {
        root.printNode();

        if(root.isInternalNode()) {
            TreeInternalNode t = (TreeInternalNode)root;
            long ptr;
            for(int i = 0; i < t.getCurrentCapacity()+1; i++) {
                ptr = t.getPointerAt(i);
                if(ptr < 0) {break;}
                printNodeAt(ptr);
            }
        }

    }

    public void printNodeAt(long index) throws IOException {
        TreeNode t = readNode(index);
        t.printNode();

        if(t.isInternalNode()) {
            TreeInternalNode t2 = (TreeInternalNode)t;
            long ptr;
            for(int i = 0; i < t2.getCurrentCapacity()+1; i++) {
                ptr = t2.getPointerAt(i);
                if(ptr < 0) {break;}
                printNodeAt(ptr);
            }
        }
    }

}
