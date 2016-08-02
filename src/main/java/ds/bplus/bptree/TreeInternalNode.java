package ds.bplus.bptree;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;


/**
 *
 * Class for our Internal nodes
 *
 */
@SuppressWarnings({"WeakerAccess", "unused"})
class TreeInternalNode extends TreeNode {

    private final LinkedList<Long> pointerArray;  // the pointer array

    /**
     * Create an internal node
     *
     * @param nodeType the node type parameter
     * @param pageIndex the index of the page
     */
    TreeInternalNode(TreeNodeType nodeType, long pageIndex) {
        super(nodeType, pageIndex);
        pointerArray = new LinkedList<>();
    }

    void removePointerAt(int index)
        {pointerArray.remove(index);}

    long getPointerAt(int index) {
        return((index < 0 || index >= pointerArray.size()) ? -1 : pointerArray.get(index));}

    long popPointer()
        {return(pointerArray.pop());}

    long removeLastPointer()
        {return(pointerArray.removeLast());}

    void addPointerAt(int index, long val)
        {pointerArray.add(index, val);}

    void addPointerLast(long val)
        {pointerArray.addLast(val);}

    void setPointerAt(int index, long val)
        {pointerArray.set(index, val);}

    int getPointerListSize()
        {return(pointerArray.size());}

    void pushToPointerArray(long val)
        {pointerArray.push(val);}


    /**
     *
     *  Internal node structure is as follows:
     *
     *  -- node type -- (2 bytes)
     *  -- current capacity -- (4 bytes)
     *
     *  -- Key -- (8 bytes max size)
     *
     *  -- Pointers (8 bytes max size + 1)
     *
     *  we go like: k1 -- p0 -- k2 -- p1 ... kn -- pn+1
     *
     * @param r pointer to *opened* B+ tree file
     * @throws IOException is thrown when an I/O exception is captured.
     */
    @Override
    public void writeNode(RandomAccessFile r, BPlusConfiguration conf,
                          BPlusTreePerformanceCounter bPerf)
            throws IOException {

        // update root index in the file
        if(this.isRoot()) {
            r.seek(conf.getHeaderSize()-8);
            r.writeLong(getPageIndex());
        }

        // account for the header page as well.
        r.seek(getPageIndex());

        // write the node type
        r.writeShort(getPageType());

        // write current capacity
        r.writeInt(getCurrentCapacity());

        // now write Key/Pointer pairs
        for(int i = 0; i < getCurrentCapacity(); i++) {
            r.writeLong(getKeyAt(i));       // Key
            r.writeLong(getPointerAt(i));   // Pointer
        }
        // final pointer.
        r.writeLong(getPointerAt(getCurrentCapacity()));

        // annoying correction
        if(r.length() < getPageIndex()+conf.getPageSize())
            {r.setLength(getPageIndex()+conf.getPageSize());}

        bPerf.incrementTotalInternalNodeWrites();
    }

    @Override
    public void printNode() {
        System.out.println("\nPrinting node of type: " +
                getNodeType().toString() + " with index: " +
                getPageIndex());

        System.out.println("Current node capacity is: " +
                getCurrentCapacity());

        System.out.println("\nPrinting stored Keys:");
        for(Long i : keyArray)
            {System.out.print("\t" + i.toString() + " ");}
        System.out.println("\nPrinting stored Pointers");
        for(Long i : pointerArray)
            {System.out.print(" " + i.toString() + " ");}
        System.out.println();
    }


}
