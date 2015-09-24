package ds.bplus.bptree;

import ds.bplus.util.InvalidBTreeStateException;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.InvalidPropertiesFormatException;
import java.util.LinkedList;

public class BPlusTree {

    private TreeNode root;
    private TreeNode aChild;
    private RandomAccessFile treeFile;
    private BPlusConfiguration conf;
    private LinkedList<Long> freeSlotPool;
    private long totalTreePages;
    private long maxPageNumber;
    private int deleteIterations;
    private BPlusTreePerformanceCounter bPerf = null;

    /**
     * Super basic constructor, create everything using their
     * default values...
     *
     * @throws IOException
     */
    @SuppressWarnings("unused")
    public BPlusTree() throws IOException, InvalidBTreeStateException {
        this.conf = new BPlusConfiguration();
        bPerf = new BPlusTreePerformanceCounter(false);
        initializeCommon();
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
    @SuppressWarnings("unused")
    public BPlusTree(BPlusConfiguration conf,
                     BPlusTreePerformanceCounter bPerf)
            throws IOException, InvalidBTreeStateException {
        this.conf = conf;
        this.bPerf = bPerf;
        initializeCommon();
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
    @SuppressWarnings("unused")
    public BPlusTree(BPlusConfiguration conf, String mode,
                     BPlusTreePerformanceCounter bPerf)
            throws IOException, InvalidBTreeStateException {
        this.conf = conf;
        this.bPerf = bPerf;
        initializeCommon();
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
    @SuppressWarnings("unused")
    public BPlusTree(BPlusConfiguration conf, String mode,
                     String treeFilePath, BPlusTreePerformanceCounter bPerf)
            throws IOException, InvalidBTreeStateException {
        this.conf = conf;
        this.bPerf = bPerf;
        initializeCommon();
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
    @SuppressWarnings("unused")
    public void insertKey(long key, String value, boolean unique)
            throws IOException, InvalidBTreeStateException {

        if(root == null)
            {throw new IllegalStateException("Can't insert to null tree");}

        if(key < 0)
            {throw new NumberFormatException("Can't have negative keys, sorry.");}

        value = conditionString(value);

        // check if our root is full
        if(root.isFull(conf)) {
            // allocate a new *internal* node, to be placed as the
            // *left* child of the new root
            aChild = this.root;
            TreeInternalNode tbuf = new TreeInternalNode(TreeNodeType.TREE_ROOT_INTERNAL,
                    generateFirstAvailablePageIndex(conf));
            tbuf.addPointerAt(0, aChild.getPageIndex());
            this.root = tbuf;

            // split root.
            splitTreeNode(tbuf, 0);
            writeFileHeader(conf);
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
    private void splitTreeNode(TreeInternalNode n, int index)
            throws IOException, InvalidBTreeStateException {

//        System.out.println("-- Splitting node with index: " +
//                aChild.getPageIndex() + " of type: " +
//                aChild.getNodeType().toString());

        int setIndex;
        TreeNode znode;
        long keyToAdd;
        TreeNode ynode = aChild; // x.c_{i}
        if(ynode.isInternalNode()) {
            TreeInternalNode zInternal,
                             yInternal = (TreeInternalNode) ynode;

            zInternal = new TreeInternalNode(TreeNodeType.TREE_INTERNAL_NODE,
                    generateFirstAvailablePageIndex(conf));

            bPerf.incrementTotalInternalNodes();


            setIndex =  conf.getTreeDegree()-1;

            int i;
            for(i = 0; i < setIndex; i++) {
                zInternal.addToKeyArrayAt(i, yInternal.popKey());
                zInternal.addPointerAt(i, yInternal.popPointer());
            }
            zInternal.addPointerAt(i, yInternal.popPointer());
            //keyToAdd = ynode.getFirstKey();
            keyToAdd = ynode.popKey();

            zInternal.setCurrentCapacity(setIndex);
            yInternal.setCurrentCapacity(setIndex);

            // it it was the root, invalidate it and make it a regular internal node
            if(yInternal.isRoot()) {
                yInternal.setNodeType(TreeNodeType.TREE_INTERNAL_NODE);
                bPerf.incrementRootSplits();
            }
            bPerf.incrementInternalNodeSplits();

            // update pointer at n_{index+1}
            n.addPointerAt(index, zInternal.getPageIndex());
            // update key value at n[index]
            n.addToKeyArrayAt(index, keyToAdd);
            // adjust capacity
            n.incrementCapacity(conf);
            // update shared child pointer.
            aChild = zInternal;
            // update reference
            znode = zInternal;
        }
        // we have a leaf
        else {
            TreeLeaf zLeaf,
                     yLeaf = (TreeLeaf) ynode,
                     afterLeaf;

            zLeaf = new TreeLeaf(yLeaf.getNextPagePointer(),
                    yLeaf.getPageIndex(), TreeNodeType.TREE_LEAF,
                    generateFirstAvailablePageIndex(conf));

            // update the previous pointer from the node after ynode
            if(yLeaf.getNextPagePointer() != -1) {
                afterLeaf = (TreeLeaf) readNode(yLeaf.getNextPagePointer());
                afterLeaf.setPrevPagePointer(zLeaf.getPageIndex());
                afterLeaf.writeNode(treeFile, conf, bPerf);
            }

            bPerf.incrementTotalLeaves();

            // update pointers in ynode, only have to update next pointer
            yLeaf.setNextPagePointer(zLeaf.getPageIndex());

            setIndex = conf.getLeafNodeDegree()-1;

            for(int i = 0; i < setIndex; i++) {
                //long fk = ynode.getLastKey();
                //long ovf1 = ((TreeLeaf)ynode).getLastOverflowPointer();
                zLeaf.pushToKeyArray(yLeaf.removeLastKey());
                zLeaf.pushToValueList(yLeaf.removeLastValue());
                zLeaf.pushToOverflowList(yLeaf.removeLastOverflowPointer());
                zLeaf.incrementCapacity(conf);
                yLeaf.decrementCapacity(conf);
            }

            // it it was the root, invalidate it and make it a regular leaf
            if(yLeaf.isRoot()) {
                yLeaf.setNodeType(TreeNodeType.TREE_LEAF);
                bPerf.incrementRootSplits();
            }
            bPerf.incrementTotalLeafSplits();

            // update pointer at n_{index+1}
            n.addPointerAt(index + 1, zLeaf.getPageIndex());
            // update key value at n[index]
            n.addToKeyArrayAt(index, zLeaf.getKeyAt(0));
            // adjust capacity
            n.incrementCapacity(conf);
            // update reference
            znode = zLeaf;
        }

        znode.setBeingDeleted(false);
        // commit the changes
        znode.writeNode(treeFile, conf, bPerf);
        ynode.writeNode(treeFile, conf, bPerf);
        n.writeNode(treeFile, conf, bPerf);
        // commit page counts
        updatePageIndexCounts(conf);
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
     * @throws IOException
     */
    private void createOverflowPage(TreeNode n, int index, String value)
            throws IOException, InvalidBTreeStateException {
        TreeOverflow novf;
        if(n.isOverflow()) {
            TreeOverflow ovf = (TreeOverflow)n;
            novf = new TreeOverflow(-1L, ovf.getPageIndex(),
                    generateFirstAvailablePageIndex(conf));
            // push the first value
            novf.pushToValueList(value);
            novf.incrementCapacity(conf);
            // update overflow pointer to parent node
            ovf.setNextPagePointer(novf.getPageIndex());
            // set being deleted to false
            novf.setBeingDeleted(false);
            // commit changes to new overflow page
            novf.writeNode(treeFile, conf, bPerf);
            // commit changes to old overflow page
            ovf.writeNode(treeFile, conf, bPerf);
        } else if(n.isLeaf()) {
            TreeLeaf l = (TreeLeaf)n;
            novf = new TreeOverflow(-1L, l.getPageIndex(),
                    generateFirstAvailablePageIndex(conf));
//            System.out.println("Creating overflow page with index: " + novf.getPageIndex() +
//                    " for key: " + l.getKeyAt(index));
            // push the first value
            novf.pushToValueList(value);
            novf.incrementCapacity(conf);
            // update overflow pointer to parent node
            l.setOVerflowPointerAt(index, novf.getPageIndex());
            // set being deleted to false
            novf.setBeingDeleted(false);
            // commit changes to overflow page
            novf.writeNode(treeFile, conf, bPerf);
            // commit changes to leaf page
            l.writeNode(treeFile, conf, bPerf);
            // commit page counts
        } else {
            throw new InvalidBTreeStateException("Expected Leaf or Overflow, " +
                    "got instead: " + n.getNodeType().toString());
        }

        bPerf.incrementTotalOverflowPages();
        // commit page counts
        updatePageIndexCounts(conf);
    }

    /**
     * This function is inspired from the one given in CLRS for inserting a key to
     * a B-Tree but as splitTreeNode has been (heavily) modified in order to be used
     * in our B+ Tree. It supports handling duplicate keys (if enabled) as well.
     *
     * It is able to insert the (Key, Value) pairs using only one pass through the tree.
     *
     * @param n current node
     * @param key key to add
     * @param value value paired with the key
     * @param unique allow duplicate entries for this time?
     * @throws IOException
     */
    private void insertNonFull(TreeNode n, long key, String value, boolean unique)
            throws IOException, InvalidBTreeStateException {
        boolean useChild = true;
        int i = n.getCurrentCapacity();
        // descend down the node
        while(i > 0 && key < n.getKeyAt(i-1)) {i--;}
        // check if we have a leaf
        if(n.isLeaf()) {
            TreeLeaf l = (TreeLeaf)n;

            // before we add it, let's check if the key already exists
            // and if it does pull up (or create) the overflow page and
            // add the value there.
            //
            // Not that we do *not* add the key if we have a true unique flag


            // this is to adjust for a corner case due to indexing
            int iadj = (n.getCurrentCapacity() > 0 &&
                    i == 0 && n.getFirstKey() > key) ? i : i-1;

            if(n.getCurrentCapacity() > 0 && n.getKeyAt(iadj) == key) {

                if(unique) {
                    //System.out.println("Duplicate entry found and unique " +
                    //        "flag enabled, can't add");
                    return;
                }

                //System.out.println("Duplicate found! Adding to overflow page!");

                // overflow page does not exist, yet; time to create it!
                if(l.getOverflowPointerAt(iadj) < 0) {
                    createOverflowPage(l, iadj, value);
                }
                // page already exists, so pull it and check if it has
                // available space, if it does all is good; otherwise we
                // pull the next overflow page or we create another one.
                else {
                    TreeOverflow ovf = (TreeOverflow)readNode(l.getOverflowPointerAt(iadj));

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
                    ovf.incrementCapacity(conf);
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
                l.incrementCapacity(conf);
                // commit the changes
                l.writeNode(treeFile, conf, bPerf);
            }

        } else {

            // This requires a bit of explanation; the above while loop
            // starts initially from the *end* key parsing the *right*
            // child, so initially it's like this:
            //
            //  Step 0:
            //
            //  Key Array:          | - | - | - | x |
            //  Pointer Array:      | - | - | - | - | x |
            //
            // Now if the while loop stops there, we have a *right* child
            // pointer, but should it continues we get the following:
            //
            // Step 1:
            //
            //  Key Array:          | - | - | x | - |
            //  Pointer Array:      | - | - | - | x | - |
            //
            //  and finally we reach the special case where we have the
            //  following:
            //
            // Final step:
            //
            //  Key Array:          | x | - | - | - |
            //  Pointer Array:      | x | - | - | - | - |
            //
            //
            // In this case we have a *left* pointer, which can be
            // quite confusing initially... hence the changed naming.
            //
            //

            TreeInternalNode inode = (TreeInternalNode)n;
            aChild = readNode(inode.getPointerAt(i));
            TreeNode nextAfterAChild = null;
            if(aChild.isFull(conf)) {
                splitTreeNode(inode, i);
                if (key >= n.getKeyAt(i)) {
                    useChild = false;
                    nextAfterAChild = readNode(inode.getPointerAt(i+1));
                }
            }

            insertNonFull(useChild ? aChild : nextAfterAChild, key, value, unique);
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
            throws IOException, InvalidBTreeStateException {
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
     * @throws IOException, InvalidBTreeStateException
     */
    @SuppressWarnings("unused")
    public SearchResult searchKey(long key, boolean unique)
            throws IOException, InvalidBTreeStateException {
        bPerf.incrementTotalSearches();
        return(searchKey(this.root, key, unique));
    }


    /**
     * This function performs the actual search as described in searchKey description
     * and is recursively called until we reach a leaf.
     *
     * @param node the node to poke into
     * @param key key that we want to match
     * @param unique unique results?
     * @return the search result
     * @throws IOException, InvalidBTreeStateException
     */
    private SearchResult searchKey(TreeNode node, long key, boolean unique)
            throws IOException {

        int i = 0;

        while(i < node.getCurrentCapacity() && key > node.getKeyAt(i)) {i++;}

        // check if we found it
        if(node.isLeaf()) {
            //i--;
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
            // padding to account for the last pointer (if needed)
            if(i != node.getCurrentCapacity() && key >= node.getKeyAt(i)) {i++;}
            TreeNode t = readNode(((TreeInternalNode)node).getPointerAt(i));
            return(searchKey(t, key, unique));
        }

    }

    /**
     * Function to delete a key from our tree... this function is again
     * adopted from CLRS delete method but this was basically written
     * from scratch and is loosely based on the actual notes.
     *
     * -- firstly it deletes the keys (all or first depending on unique flag)
     * -- secondly it updates the pool of empty pages
     * -- thirdly performs merges if necessary (it capacity falls < degree-1)
     * -- finally condenses the file size depending on load
     *
     * @param key key to delete
     * @param unique unique deletions?
     * @return the number of deleted keys
     */
    @SuppressWarnings("unused")
    public DeleteResult deleteKey(long key, boolean unique)
    throws IOException, InvalidBTreeStateException  {
        if(root.isEmpty()) {
            LinkedList<String> l = null;
            return(new DeleteResult(key, l));
        } else
            {return(deleteKey(root, null, -1, -1, key, unique));}
    }

    /**
     * That function does the job as described above; to perform easily the deletions
     * we store *two* nodes instead on *one* in memory; the parent and the current.
     * This also avoids adding information to the actual node file structure to account
     * for the "parenting" link.
     *
     * @param current node that we currently probe
     * @param parent parent of the current node
     * @param key key to delete
     * @param unique unique deletions?
     * @return the number of deleted keys
     */
    public DeleteResult deleteKey(TreeNode current, TreeInternalNode parent,
                                  int parentPointerIndex, int parentKeyIndex,
                                  long key, boolean unique)
            throws IOException, InvalidBTreeStateException {

        // check if we need to consolidate
        if(current.isTimeToMerge(conf)) {
            System.out.println("Parent needs merging (internal node)");
            TreeNode mres = mergeOrRedistributeTreeNodes(current, parent,
                    parentPointerIndex, parentKeyIndex);
            if(mres != null)
                {current = mres;}
        }

        int i = 0;
        LinkedList<String> rvals = null;
        while(i < current.getCurrentCapacity()-1 && key > current.getKeyAt(i)) {i++;}

        // check if it's an internal node
        if(current.isInternalNode()) {
            // if it is, descend to a leaf
            TreeInternalNode inode = (TreeInternalNode)current;
            int idx = i;
            // check if we are at the end
            if(key >= current.getKeyAt(i)) {
                idx++;
            }
            // read the next node
            TreeNode next = readNode(inode.getPointerAt(idx));
            // finally return the resulting set
            return(deleteKey(next, inode, idx, i/*keyLocation*/, key, unique));
        }
        // the current node, is a leaf.
        else if(current.isLeaf()) {
            TreeLeaf l = (TreeLeaf)current;
            // check if we actually found the key
            if(i == l.getCurrentCapacity()) {
                System.out.println("Key with value: " + key +
                        " not found, reached limits");
                return (new DeleteResult(key, rvals));
            } else if(key != l.getKeyAt(i)) {
                System.out.println("Key with value: " + key + " not found, key mismatch");
                //throw new InvalidBTreeStateException("Key not found!");
                return (new DeleteResult(key, rvals));
            }
            else {

                System.out.println("Found the key " + key + ", removing it");
                rvals = new LinkedList<>();

                // we we *have* to make a choice on where to make
                // a read trade off.

                // check if we have an overflow page
                if(l.getOverflowPointerAt(i) != -1) {
                    TreeOverflow ovf = null;
                    TreeOverflow povf =
                            (TreeOverflow)readNode(l.getOverflowPointerAt(i));

                    // handle singular deletes
                    if(unique) {

                        // descend to the last page
                        while(povf.getNextPagePointer() != -1L) {
                            ovf = povf;
                            povf = (TreeOverflow)readNode(povf.getNextPagePointer());
                        }

                        // remove from the overflow page the value
                        rvals.add(povf.removeLastValue());
                        povf.decrementCapacity(conf);

                        // if the page is empty, delete it.
                        if(povf.isEmpty()) {
                            if(ovf == null) {
                                l.setOVerflowPointerAt(i, -1L);
                                l.writeNode(treeFile, conf, bPerf);
                            }
                            else {
                                ovf.setNextPagePointer(-1L);
                                ovf.writeNode(treeFile, conf, bPerf);
                            }
                            // now delete the page
                            deletePage(povf.getPageIndex(), false);
                        }
                        // we don't have to delete the page, so let's
                        // update it instead.
                        else
                            {povf.writeNode(treeFile, conf, bPerf);}

                        // return the result
                        return(new DeleteResult(key, rvals));
                    }
                    // we have to delete all the values
                    else {

                        // here to save reads/writes we just
                        // "delete-as-we-read"
                        while(povf.getCurrentCapacity() > 0) {
                            rvals.add(povf.removeLastValue());
                            povf.decrementCapacity(conf);

                            // check if it's time to remove the page
                            if(povf.isEmpty()) {
                                deletePage(povf.getPageIndex(), false);
                                if(povf.getNextPagePointer() != -1L)
                                    {povf = (TreeOverflow)readNode(povf.getNextPagePointer());}
                            }
                        }
                    }

                }
            }
            // we reached here because either we have no overflow page
            // or non-unique deletes with overflow pages. We should
            // reach this point after we purged all the overflow pages.
            rvals.add(((TreeLeaf)current).removeEntryAt(i, conf));
            current.writeNode(treeFile, conf, bPerf);
        }
        else {
            throw new IllegalStateException("Read unknown or " +
                    "overflow page while descending");
        }

        return(new DeleteResult(key, rvals));
    }


    /**
     * Check if the node has the specified parent
     *
     * @param node node can be internal or leaf
     * @param parent parent is always internal node
     * @param pindex index to check
     * @return true if it is, false if it's not
     */
    private boolean isParent(TreeNode node, TreeInternalNode parent, int pindex) {
        return(parent.getCurrentCapacity() >= pindex && pindex >= 0 &&
                (node.getPageIndex() == parent.getPointerAt(pindex)));
    }

    /**
     * Simple helper function to check if we can re-distribute the node
     * values.
     *
     * @param with node to check the capacity
     * @return the number of positions to check
     * @throws InvalidBTreeStateException
     */
    private int canRedistribute(TreeNode with)
            throws InvalidBTreeStateException {
        if(with != null) {
            if(with.isInternalNode()) {
                if(isValidAfterRemoval(((TreeInternalNode)with), 1)) {return(1);}
            } else if(with.isLeaf()) {
                if(isValidAfterRemoval(((TreeLeaf)with), 1)) {return(1);}
            } else
                {throw new InvalidBTreeStateException("Not leaf or internal node found");}
        }
        return(-1);
    }

    /**
     * Check if the internal node fulfills the B+ Tree invariant after removing
     * <code>remove</code> number of elements
     *
     * @param node node to check
     * @param remove elements to be removed
     * @return true if does, false if it fails the condition
     */
    private boolean isValidAfterRemoval(TreeInternalNode node, int remove)
        {return((node.getCurrentCapacity()-remove) >= conf.getMinInternalNodeCapacity());}

    /**
     * Check if the leaf node fulfills the B+ Tree invariant after removing
     * <code>remove</code> number of elements
     *
     * @param node node to check
     * @param remove elements to be removed
     * @return true if does, false if it fails the condition
     */
    private boolean isValidAfterRemoval(TreeLeaf node, int remove)
        {return((node.getCurrentCapacity()-remove) >= conf.getMinLeafNodeCapacity());}

    /**
     * Function that is responsible to redistribute values among two leaf nodes
     * while updating the referring key of the parent node (always an internal node).
     *
     *
     * We have two distinct cases which are the following:
     *
     * This case is when we use the prev pointer:
     *
     * |--------|  <-----  |--------|
     * |  with  |          |   to   |
     * |--------|  ----->  |--------|
     *
     * In this case we  *remove* the *last* n elements from with
     * and *push* them (in the order removed) into the destination node
     *
     * The parent key-pointer is updated with the first value of the
     * receiving node.
     *
     * The other case is when we use the next pointer:
     *
     * |--------|  <-----  |--------|
     * |   to   |          |  with  |
     * |--------|  ----->  |--------|
     *
     * In this case we *remove* the *first* n elements from with and
     * add them *last* (in the order removed) into the destination node.
     *
     * The parent key-pointer is updated with the first value of the
     * node we retrieved the values.
     *
     *
     *
     * @param to node to receive (Key, Value) pairs
     * @param with node that we take the (Key, Value) pairs
     * @param left if left is true, then we use prev leaf else next
     * @param parent the internal node parenting both
     * @param parentKeyIndex index of the parent that refers to this pair
     */
    private void redistributeNodes(TreeLeaf to, TreeLeaf with,
                                   boolean left, TreeInternalNode parent,
                                   int parentKeyIndex)
            throws IOException, InvalidBTreeStateException {
        long key;
        // handle the case when redistributing using prev
        if(left) {
            to.pushToOverflowList(with.removeLastOverflowPointer());
            to.pushToValueList(with.removeLastValue());
            to.pushToKeyArray(with.removeLastKey());
            to.incrementCapacity(conf);
            with.decrementCapacity(conf);
            // get the key from the left node
            key = to.getKeyAt(0);

        }
        // handle the case when redistributing using next
        else {
            to.addLastToOverflowList(with.popOverflowPointer());
            to.addLastToValueList(with.popValue());
            to.addLastToKeyArray(with.popKey());
            to.incrementCapacity(conf);
            with.decrementCapacity(conf);
            // get the key from the right node
            key = with.getKeyAt(0);
        }

        // in either case update parent pointer
        parent.setKeyArrayAt(parentKeyIndex, key);
        // finally write the changes
        to.writeNode(treeFile, conf, bPerf);
        with.writeNode(treeFile, conf, bPerf);
        parent.writeNode(treeFile, conf, bPerf);
    }

    /**
     * Function that is responsible to redistribute values among two internal nodes
     * while updating the referring key of the parent node (always an internal node).
     *
     *
     * We have two distinct cases which are the following:
     *
     * This case is when we use the prev pointer:
     *
     * |--------|  <-----  |--------|
     * |  with  |          |   to   |
     * |--------|  ----->  |--------|
     *
     * In this case we  *remove* the *last* n elements from with
     * and *push* them (in the order removed) into the destination node
     *
     * The parent key-pointer is updated with the first value of the
     * receiving node.
     *
     * The other case is when we use the next pointer:
     *
     * |--------|  <-----  |--------|
     * |   to   |          |  with  |
     * |--------|  ----->  |--------|
     *
     * In this case we *remove* the *first* n elements from with and
     * add them *last* (in the order removed) into the destination node.
     *
     * The parent key-pointer is updated with the first value of the
     * node we retrieved the values.
     *
     *
     *
     * @param to node to receive (Key, Value) pairs
     * @param with node that we take the (Key, Value) pairs
     * @param left if left is true, then we use prev leaf else next
     * @param parent the internal node parenting both
     * @param parentKeyIndex index of the parent that refers to this pair
     */
    private void redistributeNodes(TreeInternalNode to, TreeInternalNode with,
                                   boolean left, TreeInternalNode parent,
                                   int parentKeyIndex)
            throws IOException, InvalidBTreeStateException {
        long key, pkey = parent.getKeyAt(parentKeyIndex);
        if(left) {
            to.pushToKeyArray(pkey);
            key = with.removeLastKey();
            to.pushToPointerArray(with.removeLastPointer());
            to.incrementCapacity(conf);
            with.decrementCapacity(conf);
        }
        // handle the case when redistributing using next
        else {
            to.addLastToKeyArray(pkey);
            key = with.popKey();
            to.addPointerLast(with.popPointer());
            to.incrementCapacity(conf);
            with.decrementCapacity(conf);
        }
        // in either case update the parent key
        parent.setKeyArrayAt(parentKeyIndex, key);
        // finally write the chances
        to.writeNode(treeFile, conf, bPerf);
        with.writeNode(treeFile, conf, bPerf);
        parent.writeNode(treeFile, conf, bPerf);
    }


    /**
     * Function that merges two leaves together; in this case we *must*
     * have two leaves, left and right that are merged and their parent
     * that *must* be an internal node (or root).
     *
     * We also have the index of the parent that the pointers indicate
     * these two leaves
     *
     * it should be like this:
     *
     *            parent
     *      ... |  key  | ...
     *           /     \
     *      | left | right |
     *
     *
     * The merge happens from right -> left thus the final result would
     * be like this:
     *
     *            parent
     *      ... |  key  | ...
     *           /     \
     *      | result |  x
     *
     *
     * So basically we dump the values of right to the left while
     * updating the pointers.
     *
     *
     *
     * @param left left-most leaf to merge
     * @param right right-most leaf to merge
     * @param other the other cached node
     * @param parent parent of both leaves (internal node)
     * @param parentPointerIndex index of parent that has these two pointers
     */
    private TreeNode mergeNodes(TreeLeaf left, TreeLeaf right, TreeLeaf other,
                            TreeInternalNode parent, int parentPointerIndex,
                            int parentKeyIndex, boolean isLeftOfNext,
                                boolean useNextPointer)
            throws IOException, InvalidBTreeStateException {

        if((left.getCurrentCapacity() + right.getCurrentCapacity()) >
                conf.getMaxLeafNodeCapacity()) {
            throw new InvalidBTreeStateException("Leaf node capacity exceeded in merge");
        }

        // flag the node for deletion
        right.setBeingDeleted(true);
        // join the two leaves together.
        int cap = right.getCurrentCapacity();
        for(int i = 0; i < cap; i++) {
            left.addLastToOverflowList(right.popOverflowPointer());
            left.addLastToValueList(right.popValue());
            left.addLastToKeyArray(right.popKey());
            left.incrementCapacity(conf);
            right.decrementCapacity(conf);
        }

        // update the next pointers
        left.setNextPagePointer(right.getNextPagePointer());
        // now fix the top pointer
        if(useNextPointer) {
            if(isLeftOfNext) {
                parent.removeKeyAt(parentKeyIndex+1);
                parent.removePointerAt(parentPointerIndex+1);
            } else {
                parent.removeKeyAt(parentKeyIndex);
                parent.removePointerAt(parentPointerIndex+1);
            }
        } else {
            if(isLeftOfNext) {
                parent.removeKeyAt(parentKeyIndex);
                parent.removePointerAt(parentPointerIndex);
            } else {
                parent.removeKeyAt(parentKeyIndex);
                parent.removePointerAt(parentPointerIndex);
                parent.setKeyArrayAt(parentKeyIndex-1, other.getFirstKey());
            }
        }

        // update capacity as in both cases we remove a value
        parent.decrementCapacity(conf);
        // write parent node
        parent.writeNode(treeFile, conf, bPerf);

        // update the prev pointer of right next node (if any)
        if(right.getNextPagePointer() != -1) {
            TreeLeaf rnext = (TreeLeaf)readNode(right.getNextPagePointer());
            rnext.setPrevPagePointer(left.getPageIndex());
            rnext.writeNode(treeFile, conf, bPerf);
        }

        // write the left node to disk
        left.writeNode(treeFile, conf, bPerf);
        // remove the page
        deletePage(right.getPageIndex(), false);
        // finally return the node reference
        return(left);
    }

    /**
     * Naive merge of two leaf nodes from right to left.
     *
     * @param left node that merge ends
     * @param right node that is deleted during merge
     * @throws IOException
     */
    private void mergeNodes(TreeLeaf left, TreeLeaf right)
            throws IOException, InvalidBTreeStateException {
        right.setBeingDeleted(true);
        // join the two leaves together.
        int cap = right.getCurrentCapacity();
        for(int i = 0; i < cap; i++) {
            left.addLastToOverflowList(right.popOverflowPointer());
            left.addLastToValueList(right.popValue());
            left.addLastToKeyArray(right.popKey());
            left.incrementCapacity(conf);
            right.decrementCapacity(conf);
        }

        // remove the page
        deletePage(right.getPageIndex(), false);
    }

    /**
     * Function that merges two internal nodes together; in this case
     * we *must* have two internal nodes, left and right that are merged
     * and their parent that *must* be an internal node (or root).
     *
     * We also have the index of the parent that the pointers
     * indicate these two internal nodes.
     *
     * it should be like this:
     *
     *            parent
     *      ... |  key  | ...
     *           /     \
     *      | left | right |
     *
     * @param left left-most internal node to merge
     * @param right right-most internal node to merge
     * @param parent parent of both internal nodes.
     * @param parentPointerIndex index of the parent that has these two pointers
     */
    private TreeNode mergeNodes(TreeInternalNode left, TreeInternalNode right,
                            TreeInternalNode other, TreeInternalNode parent,
                                int parentPointerIndex,
                                int parentKeyIndex,
                                boolean isLeftOfNext,
                                boolean useNextPointer)
            throws IOException, InvalidBTreeStateException {

        // check if we can actually merge
        if((left.getCurrentCapacity() + right.getCurrentCapacity()) >
                conf.getMaxInternalNodeCapacity()) {
            throw new InvalidBTreeStateException("Internal node capacity exceeded in merge");
        }

        // check if we are using a trickery
        if(isLeftOfNext && useNextPointer)
            {left.addLastToKeyArray(parent.getKeyAt(parentKeyIndex+1));}
        else
            {left.addLastToKeyArray(parent.getKeyAt(parentKeyIndex));}

        // flag the node for deletion
        right.setBeingDeleted(true);

        int cap = right.getCurrentCapacity();
        for(int i = 0; i < cap; i++) {
            left.addLastToKeyArray(right.popKey());
            left.addPointerLast(right.popPointer());
            left.incrementCapacity(conf);
            right.decrementCapacity(conf);
        }
        // pump the last pointer as well
        left.addPointerLast(right.popPointer());
        // now increment the capacity as well
        left.incrementCapacity(conf);
        // now fix the top pointer.


        // now fix the top pointer
        if(useNextPointer) {
            if(isLeftOfNext) {
                parent.removeKeyAt(parentKeyIndex+1);
                parent.removePointerAt(parentPointerIndex+1);
            } else {
                parent.removeKeyAt(parentKeyIndex);
                parent.removePointerAt(parentPointerIndex+1);
            }
        } else {
            if(isLeftOfNext) {
                parent.removeKeyAt(parentKeyIndex);
                parent.removePointerAt(parentPointerIndex);
            } else {
                parent.removeKeyAt(parentKeyIndex);
                parent.removePointerAt(parentPointerIndex);
                parent.setKeyArrayAt(parentKeyIndex-1, other.getFirstKey());
            }
        }

        // update capacity as in both cases we remove a value
        parent.decrementCapacity(conf);
        // write parent node
        parent.writeNode(treeFile, conf, bPerf);

        // write the node
        left.writeNode(treeFile, conf, bPerf);
        // remove the page
        deletePage(right.getPageIndex(), false);
        // finally remove the node reference
        return(left);
    }

    /**
     * Naive merge of two internal nodes from right to left.
     *
     * @param left node that merge ends
     * @param right node that is deleted during merge
     * @throws IOException
     */
    private void mergeNodes(TreeInternalNode left, TreeInternalNode right, long midKey)
            throws IOException, InvalidBTreeStateException {
        right.setBeingDeleted(true);
        left.addLastToKeyArray(midKey);
        int cap = right.getCurrentCapacity();
        for(int i = 0; i < cap; i++) {
            left.addLastToKeyArray(right.popKey());
            left.addPointerLast(right.popPointer());
            left.incrementCapacity(conf);
            right.decrementCapacity(conf);
        }

        // pump the last pointer as well
        left.addPointerLast(right.popPointer());
        left.incrementCapacity(conf);

        // finally remove the page
        deletePage(right.getPageIndex(), false);
    }

    /**
     *
     * This function handles the merging or redistribution of the nodes depending
     * on their capacity; usually we just redistribute, if not we merge.
     *
     * @param mnode current node (either leaf or internal node)
     * @param parent parent node *always* an internal node
     * @param parentPointerIndex pointer index to mnode in parent
     * @param parentKeyIndex key index that has mnode as a child in parent
     * @return the updated mnode
     * @throws IOException
     * @throws InvalidBTreeStateException
     */
    public TreeNode mergeOrRedistributeTreeNodes(TreeNode mnode, TreeInternalNode parent,
                                             int parentPointerIndex, int parentKeyIndex)
            throws IOException, InvalidBTreeStateException {

        // that index should not be present
        if(parent != null && parentPointerIndex < 0)
            {throw new IllegalStateException("index < 0");}

        // this is the only case that the tree shrinks, from the root.
        if(parent == null) {
            TreeNode lChild = handleRootRedistributionOrMerging(mnode);
            if (lChild != null) return lChild;
        }
        // merging a leaf requires the most amount of work, since
        // all leaves by definition are linked in a doubly-linked
        // linked-list; hence when we merge/remove a node we have
        // to make sure those links are consistent
        else if(mnode.isLeaf()) {
            return handleLeafNodeRedistributionOrMerging(mnode, parent, parentPointerIndex, parentKeyIndex);
        }

        // we have to merge internal nodes, this is the somewhat easy
        // case, since we do not have to update any more links than the
        // currently pulled nodes.
        else if(mnode.isInternalNode()) {
            return handleInternalNodeRedistributionOrMerging(mnode, parent, parentPointerIndex, parentKeyIndex);
        } else
            {throw new IllegalStateException("Read unknown or overflow page while merging");}

        // probably we didn't change something
        return null;
    }

    /**
     * Handle the root node cases (leaf and internal node)
     *
     * @param mnode root node
     * @return the root node
     * @throws IOException
     * @throws InvalidBTreeStateException
     */
    private TreeNode handleRootRedistributionOrMerging(TreeNode mnode)
            throws IOException, InvalidBTreeStateException {
        if(mnode.isInternalNode()) {
            System.out.println("\n -- Check if Consolidating Root required");

            if(mnode.getCurrentCapacity() > 1)
                {System.out.println("\n -- Root size > 2, no consolidation"); return root;}

            TreeInternalNode splitNode = (TreeInternalNode)mnode;
            // read up their pointers
            TreeNode lChild, rChild;

            // load the pointers
            lChild = readNode(splitNode.getPointerAt(0));
            rChild = readNode(splitNode.getPointerAt(1));

            if(lChild == null || rChild == null)
                {throw new InvalidBTreeStateException("Null root child found.");}

            int lcap = lChild.getCurrentCapacity();
            int rcap = rChild.getCurrentCapacity();

            // check their type
            if(lChild.isLeaf()) {

                // check if it's time to merge
                if((lcap > conf.getMinLeafNodeCapacity()) &&
                        (rcap > conf.getMinLeafNodeCapacity()))
                    {System.out.println(" -- No need to consolidate root yet (to -> leaf)"); return mnode;}

                TreeInternalNode rNode = (TreeInternalNode)mnode;
                TreeLeaf pLeaf = (TreeLeaf)lChild,
                         nLeaf = (TreeLeaf)rChild;

                // now merge them, and delete root page, while
                // updating it's page number etc

                int nnum = canRedistribute(nLeaf);
                int pnum = canRedistribute(pLeaf);

                if(nnum > 0) {
                    redistributeNodes(pLeaf, nLeaf, false, rNode, 0);
                }
                // now check if we can redistribute with prev
                else if(pnum > 0) {
                    redistributeNodes(nLeaf, pLeaf, true, rNode, 0);
                }
                // merge the two nodes and promote them to root
                else {
                    mergeNodes(pLeaf, nLeaf);
                    // update root page
                    pLeaf.setNodeType(TreeNodeType.TREE_ROOT_LEAF);
                    // update the leaf pointers
                    pLeaf.setNextPagePointer(-1L);
                    pLeaf.setPrevPagePointer(-1L);
                    // delete previous root page
                    deletePage(root.getPageIndex(), false);
                    // set root page
                    root = lChild;
                    // write root header
                    writeFileHeader(conf);
                    // write left leaf
                    lChild.writeNode(treeFile, conf, bPerf);
                    // since we have a new root
                    return(lChild);
                }

            } else if(lChild.isInternalNode()) {
                // check if it's time to merge
                if((lcap+rcap) >= conf.getMaxInternalNodeCapacity())
                {System.out.println(" -- No need to consolidate root yet (to -> internal)"); return mnode;}
                else {
                    System.out.println("-- Consolidating Root (internal -> internal)");
                }

                TreeInternalNode rNode = (TreeInternalNode)mnode,
                                 lIntNode = (TreeInternalNode)lChild,
                                 rIntNode = (TreeInternalNode)rChild;

                int nnum = canRedistribute(rIntNode);
                int pnum = canRedistribute(lIntNode);


                if(nnum > 0) {
                    redistributeNodes(lIntNode, rIntNode, true, rNode, 0);
                }
                // now check if we can redistribute with prev
                else if(pnum > 0) {
                    System.out.println("\t -- Redistributing right with elements from left");
                    redistributeNodes(rIntNode, lIntNode, false, rNode, 0);
                }
                // merge the two nodes and promote them to root
                else {
                    System.out.println("\t -- Merging leaf nodes");
                    mergeNodes(lIntNode, rIntNode, splitNode.getFirstKey());
                    // update root page
                    lIntNode.setNodeType(TreeNodeType.TREE_ROOT_INTERNAL);
                    // delete previous root page
                    deletePage(root.getPageIndex(), false);
                    // set root page
                    root = lChild;
                    // write root header
                    writeFileHeader(conf);
                    // write left leaf
                    lChild.writeNode(treeFile, conf, bPerf);
                    // since we have a new root
                    return(lChild);
                }
            } else
                {throw new InvalidBTreeStateException("Unknown children type");}

            return(root);

        } else if(!mnode.isLeaf())
            {throw new InvalidBTreeStateException("Invalid tree Root type.");}
        return null;
    }

    /**
     * Handle the leaf section of the redistribution/merging
     *
     * @param mnode node to process
     * @param parent the parent node
     * @param parentPointerIndex parent pointer index
     * @param parentKeyIndex parent key index that mnode is child
     * @return the updated node (if any updates are made)
     * @throws IOException
     * @throws InvalidBTreeStateException
     */
    private TreeNode handleLeafNodeRedistributionOrMerging(TreeNode mnode,
                                                           TreeInternalNode parent,
                                                           int parentPointerIndex,
                                                           int parentKeyIndex)
            throws IOException, InvalidBTreeStateException {
        TreeLeaf splitNode = (TreeLeaf)mnode;
        TreeLeaf nptr, pptr;

        // load the pointers
        nptr = (TreeLeaf)readNode(splitNode.getNextPagePointer());
        pptr = (TreeLeaf)readNode(splitNode.getPrevPagePointer());

        if(nptr == null && pptr == null) {
            throw new InvalidBTreeStateException("Both children (leaves) can't null");
        } else {
            System.out.println("Leaf node merging/redistribution needs to happen");
        }

        // validate neighbouring nodes
        validateNeighbours(pptr, splitNode, nptr);

        int nnum = canRedistribute(nptr);
        int pnum = canRedistribute(pptr);
        int snum = canRedistribute(splitNode);
        boolean isLeftOfNext = (parentPointerIndex > parentKeyIndex);
        boolean splitNodeIsLeftChild = parentKeyIndex == parentPointerIndex;

        boolean npar = isParent(nptr, parent, parentPointerIndex+1);
        boolean ppar = isParent(pptr, parent, parentPointerIndex-1);

        // check if we can redistribute with next
        if(nnum > 0 && npar) {
            System.out.println("\t -- Redistributing split node with elements from next");
            if(splitNodeIsLeftChild) {
                redistributeNodes(splitNode, nptr, false, parent, parentKeyIndex);
            } else {
                redistributeNodes(splitNode, nptr, false, parent, parentKeyIndex+1);
            }
        }
        // now check if we can redistribute with prev
        else if(pnum > 0 && ppar) {
            System.out.println("\t -- Redistributing split node with elements from prev");
            if(splitNodeIsLeftChild) {
                redistributeNodes(splitNode, pptr, true, parent, parentKeyIndex-1);
            } else {
                redistributeNodes(splitNode, pptr, true, parent, parentKeyIndex);
            }
        } else if(snum > 0) {
            if(nptr != null) {
                System.out.println("\t -- Redistributing next node with elements from split");
                if(splitNodeIsLeftChild) {
                    redistributeNodes(nptr, splitNode, true, parent, parentKeyIndex);
                } else {
                    redistributeNodes(nptr, splitNode, true, parent, parentKeyIndex+1);
                }
            } else {
                System.out.println("\t -- Redistributing prev with elements from split");
                if(splitNodeIsLeftChild) {
                    redistributeNodes(pptr, splitNode, false, parent, parentKeyIndex-1);
                } else {
                    redistributeNodes(pptr, splitNode, false, parent, parentKeyIndex);
                }
            }
        }
        // we can't redistribute, try merging with next
        else if(npar) {
            System.out.println("Merging leaf next");
            // it's the case where split node is the left node from parent
            mnode = mergeNodes(splitNode, nptr, pptr, parent,
                    parentPointerIndex, parentKeyIndex,
                    isLeftOfNext, /*useNextPointer = */ true);
        }
        // last chance, try merging with prev
        else if(ppar) {
            System.out.println("Merging leaf prev");
            // it's the case where split node is in the left from parent
            mnode = mergeNodes(pptr, splitNode, nptr, parent,
                    parentPointerIndex, parentKeyIndex,
                    isLeftOfNext, /*useNextPointer = */ false);
        } else
            {throw new IllegalStateException("Can't have both leaf " +
                    "pointers null and not be root or no " +
                    "common parent");}

        return(mnode);
    }

    /**
     * Extra validation on leaf pointers
     *
     * @param prev previous leaf
     * @param split current leaf (split node)
     * @param next next leaf
     * @throws InvalidBTreeStateException
     */
    private void validateNeighbours(TreeLeaf prev, TreeLeaf split, TreeLeaf next)
            throws InvalidBTreeStateException {
        if(prev != null) {
            if(split.getPrevPagePointer() != prev.getPageIndex()) {
                throw new InvalidBTreeStateException("Split prev pointer not matching prev page index");
            } else if(prev.getNextPagePointer() != split.getPageIndex()) {
                throw new InvalidBTreeStateException("Split page index not matching prev pointer");
            }
        }

        if(next != null) {
            if(split.getNextPagePointer() != next.getPageIndex()) {
                throw new InvalidBTreeStateException("Split next pointer not matching next page index");
            } else if(next.getPrevPagePointer() != split.getPageIndex()) {
                throw new InvalidBTreeStateException("Split page index not matching next pointer");
            }
        }
    }

    /**
     * Handle the internal node redistribution/merging
     *
     * @param mnode node to process
     * @param parent the parent node
     * @param parentPointerIndex parent pointer index
     * @param parentKeyIndex parent key index that mnode is child
     * @return the updated node (if any updates are made)
     * @throws IOException
     * @throws InvalidBTreeStateException
     */
    private TreeNode handleInternalNodeRedistributionOrMerging(TreeNode mnode,
                                                               TreeInternalNode parent,
                                                               int parentPointerIndex,
                                                               int parentKeyIndex)
            throws IOException, InvalidBTreeStateException {
        System.out.println("Internal node merging/redistribution needs to happen");

        TreeInternalNode splitNode = (TreeInternalNode)mnode;
        TreeInternalNode nptr, pptr;

        // load the adjacent nodes
        nptr = (TreeInternalNode)readNode(parent.getPointerAt(parentPointerIndex+1));
        pptr = (TreeInternalNode)readNode(parent.getPointerAt(parentPointerIndex-1));

        if(nptr == null && pptr == null) {
            throw new InvalidBTreeStateException("Can't have both children null");
        }

        int nnum = canRedistribute(nptr);
        int pnum = canRedistribute(pptr);
        int snum = canRedistribute(splitNode);
        boolean isLeftOfNext = (parentPointerIndex > parentKeyIndex);
        boolean splitNodeIsLeftChild = parentKeyIndex == parentPointerIndex;

        // check if we can redistribute with the next node
        if(nnum > 0) {
            System.out.println(" -- Internal Redistribution to split with next");
            if(splitNodeIsLeftChild) {
                redistributeNodes(splitNode, nptr, false, parent, parentKeyIndex);
            } else {
                redistributeNodes(splitNode, nptr, false, parent, parentKeyIndex+1);
            }
        }
        // check if we can redistribute with the previous node
        else if(pnum > 0) {
            System.out.println(" -- Internal Redistribution to split with prev");
            if(splitNodeIsLeftChild) {
                redistributeNodes(splitNode, pptr, true, parent, parentKeyIndex-1);
            } else {
                redistributeNodes(splitNode, pptr, true, parent, parentKeyIndex);
            }
        }
        else if(snum > 0) {
            // check try to send items from next
            if(nptr != null) {
                System.out.println(" -- Internal Redistribution to next with split");
                if(splitNodeIsLeftChild) {
                    redistributeNodes(nptr, splitNode, true, parent, parentKeyIndex);
                } else {
                    redistributeNodes(nptr, splitNode, true, parent, parentKeyIndex+1);
                }
            }
            // if not, try to send items from prev
            else {
                System.out.println(" -- Internal Redistribution to prev with split");
                if(splitNodeIsLeftChild) {
                    redistributeNodes(pptr, splitNode, false, parent, parentKeyIndex-1);
                } else {
                    redistributeNodes(pptr, splitNode, false, parent, parentKeyIndex);
                }
            }
        }
        // can't redistribute; merging needs to happen, check which has t-1 elements
        else {
            System.out.println(" -- Internal merging actually happens");
            // check if we can merge with the right node
            if(nptr != null && nptr.isTimeToMerge(conf)) {
                mnode = mergeNodes(splitNode, nptr, pptr, parent,
                        parentPointerIndex, parentKeyIndex,
                        isLeftOfNext, /*useNextPointer = */ true);
            }
            // now, check if we can merge with the left node
            else if(pptr != null && pptr.isTimeToMerge(conf)) {
                mnode = mergeNodes(pptr, splitNode, nptr, parent,
                        parentPointerIndex, parentKeyIndex,
                        isLeftOfNext, /*useNextPointer = */ false);
            } else {
                throw new InvalidBTreeStateException("Can't merge or " +
                        "redistribute, corrupted file?");
            }
        }

        return(mnode);
    }

    /**
     * Function that initially creates the tree. Here we always
     * create a Leaf that acts as our Root, until we split it.
     * @return the initial (leaf) tree root.
     * @throws IOException
     */
    private TreeNode createTree() throws IOException, InvalidBTreeStateException {
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
    @SuppressWarnings("unused")
    private long calculatePageOffset(long index)
        {return(conf.getPageSize()*(index+1));}

    /**
     * Function that commits the allocation pool to the file;
     * this can be done after each deletion or more unsafely
     * before committing the file changes at the end.
     *
     * @throws IOException
     */
    private void commitLookupPage() throws IOException {
        // seek to the position we have to start to write
        treeFile.seek(conf.getHeaderSize());
        Collections.sort(freeSlotPool);
        // now write
        for(long i : freeSlotPool)
            {treeFile.writeLong(i);}
    }

    /**
     * Read each tree node and return it as a generic type
     *
     * @param index index of the page in the file
     * @return a TreeNode object referencing to the loaded page
     * @throws IOException
     */
    private TreeNode readNode(long index) throws IOException {

        // caution.
        if(index < 0)
            {return(null);}

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
            // set deleted flag
            tnode.setBeingDeleted(false);
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

            // set being deleted to false
            tnode.setBeingDeleted(false);

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
            throws IOException, InvalidBTreeStateException {
        r.seek(0L);

        // read the header number
        int headerNumber = r.readInt();

        if(headerNumber < 0)
            {throw new InvalidBTreeStateException("Negative header number found...");}

        // read the page size
        int pageSize = r.readInt();

        if(pageSize < 0)
            {throw new InvalidBTreeStateException("Cannot create a tree with negative page size");}

        // read the entry size
        int entrySize = r.readInt();

        if(entrySize <= 0)
            {throw new InvalidBTreeStateException("Entry size must be > 0");}

        // key size
        int keySize = r.readInt();

        if(keySize > 8 || keySize < 4)
            {throw new InvalidBTreeStateException("Key size but be either 4 or 8 bytes");}

        // read the number of pages (excluding the lookup)
        totalTreePages = r.readLong();

        if(totalTreePages < 0)
            {throw new InvalidBTreeStateException("Tree page number cannot be < 0");}


        // read the max page offset
        maxPageNumber = r.readLong();

        if(maxPageNumber < 0 || (totalTreePages > 0 && maxPageNumber == 0))
            {throw new InvalidBTreeStateException("Invalid max page offset");}

        // read the root index
        long rootIndex = r.readLong();

        if(rootIndex < 0)
            {throw new InvalidBTreeStateException("Root can't have index < 0");}

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
        treeFile.writeLong(maxPageNumber);
        treeFile.writeLong(root.getPageIndex());
    }

    /**
     * Opens a file descriptor to our B+ Tree storage file; it can
     * handle already existing files as well without recreating them
     * unless explicitly stated.
     *
     * @param path file path
     * @param mode mode of opening (basically to truncate it or not)
     * @param opt configuration reference
     * @throws IOException
     */
    private void openFile(String path, String mode, BPlusConfiguration opt)
            throws IOException, InvalidBTreeStateException {
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
        {writeFileHeader(conf); commitLookupPage(); this.treeFile.close();}

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
        else {

            if(maxPageNumber <= totalTreePages)
                {
                    maxPageNumber++;}

            totalTreePages++;

            index = conf.getPageSize() * (maxPageNumber + 1);
            return(index);
        }
    }

    /**
     * Commit the page count and the max offset in the file
     *
     * @param conf B+ configuration reference
     * @throws IOException
     */
    private void updatePageIndexCounts(BPlusConfiguration conf) throws IOException {
        treeFile.seek(conf.getPageCountOffset());
        treeFile.writeLong(totalTreePages);
        treeFile.writeLong(maxPageNumber);
    }

    /**
     * Return the current configuration
     *
     * @return the configuration reference
     */
    @SuppressWarnings("unused")
    public BPlusConfiguration getTreeConfiguration()
        {return(conf);}


    /**
     * Return the current performance class tied to our instance
     *
     * @return the performance class reference
     */
    @SuppressWarnings("unused")
    public BPlusTreePerformanceCounter getPerformanceClass()
        {return bPerf;}

    /**
     * Prints the current configuration to stdout.
     */
    public void printCurrentConfiguration()
        {conf.printConfiguration();}

    /**
     * Returns the total number of pages currently in use
     * @return return the total number of pages
     */
    @SuppressWarnings("unused")
    public long getTotalTreePages()
        {return totalTreePages;}

    /**
     * Max index used (indicates the filesize)
     * @return max number of pages that the file has
     */
    @SuppressWarnings("unused")
    public long getMaxPageNumber()
        {return maxPageNumber;}

    @SuppressWarnings("unused")
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

    /**
     *
     * Handy method to initialize common variables
     *
     */
    private void initializeCommon() {
        this.totalTreePages = 0L;
        this.maxPageNumber = 0L;
        this.deleteIterations = 0;
        this.freeSlotPool = new LinkedList<>();
        this.bPerf.setBTree(this);
    }

    /**
     * Delete the page
     *
     * @param pageIndex page index to remove
     * @param sort sort free sort pool?
     */
    private void deletePage(long pageIndex, boolean sort)
            throws IOException {
        this.freeSlotPool.add(pageIndex);
        this.totalTreePages--;
        this.deleteIterations++;
        if(sort || isTimeForConditioning()) {
            this.deleteIterations = 0;
            Collections.sort(freeSlotPool);
            long lastPos = freeSlotPool.getLast();
            while(lastPos == calculateActualPageIndex(this.maxPageNumber)) {
                this.maxPageNumber--;
                freeSlotPool.removeLast();
                lastPos = freeSlotPool.getLast();
            }

            // set the length to be max page plus one
            treeFile.setLength(calculateActualPageIndex(this.maxPageNumber + 1));

            //TODO Commit file needs a bit more work as in low page
            //      sizes a case arises that the lookup-page pool is much
            //      larger than the available spots.
            //
            //
            // commit the lookup page to the file
            //commitLookupPage();
        }
    }

    /**
     * Check if we have to condition the file
     *
     * @return if it's time for conditioning
     */
    private boolean isTimeForConditioning()
        {return(this.deleteIterations == conf.getConditionThreshold());}

    private long calculateActualPageIndex(long index)
        {return(conf.getPageSize() * (index+1));}

    /**
     * Helper to print the node
     *
     * @param index index of the node to read and print.
     * @throws IOException
     */
    @SuppressWarnings("unused")
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


    /**
     * Condition the given string to match the entry size.
     *
     *  -- in case of length being greater than entry size, it is trimmed
     *  -- in case of length being less than entry size, it is appended with
     *      whitespaces.
     *
     * @param s string to condition
     * @return the conditioned string
     */
    private String conditionString(String s) {
        if(s == null) {
            s = " ";
            //System.out.println("Cannot have a null string");
        }

        if(s.length() > conf.getEntrySize()) {
            System.out.println("Satellite length can't exceed " +
                    conf.getEntrySize() + " trimming...");
            s = s.substring(0, conf.getEntrySize());
        } else if(s.length() < conf.getEntrySize()) {
            //System.out.println("Satellite length can't be less than" +
            //        conf.getEntrySize() + ", adding whitespaces to make up");
            int add = conf.getEntrySize() - s.length();
            for(int i = 0; i < add; i++) {s = s + " ";}
        }
        return(s);
    }

}
