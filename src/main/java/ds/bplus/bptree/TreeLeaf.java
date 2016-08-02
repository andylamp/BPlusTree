package ds.bplus.bptree;

import ds.bplus.util.InvalidBTreeStateException;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;

/**
 * Class for our Tree leafs
 *
 */
@SuppressWarnings("unused")
class TreeLeaf extends TreeNode {
    private long nextPagePointer;           // pointer to next leaf in the list
    private long prevPagePointer;           // pointer to prev leaf in the list
    private LinkedList<String> valueList;   // satellite data list
    private LinkedList<Long> overflowList;  // overflow pointer list

    /**
     * Constructor for our Internal node
     *
     * @param nextPagePointer the next leaf pointer
     * @param prevPagePointer the previous leaf pointer
     * @param nodeType the node type
     * @param pageIndex the index of the page
     */
    TreeLeaf(long nextPagePointer, long prevPagePointer,
             TreeNodeType nodeType, long pageIndex) {
        super(nodeType, pageIndex);
        if(nodeType == TreeNodeType.TREE_ROOT_LEAF && nextPagePointer > 0)
            {throw new IllegalArgumentException("Can't have leaf " +
                    "root with non-null next pointer");}
        this.nextPagePointer = nextPagePointer;
        this.prevPagePointer = prevPagePointer;
        this.overflowList = new LinkedList<>();
        this.valueList = new LinkedList<>();
    }

    void addToOverflowList(int index, long value)
        {overflowList.add(index, value);}

    void addLastToOverflowList(long value)
        {overflowList.addLast(value);}

    void addLastToValueList(String value)
        {valueList.addLast(value);}

    long getOverflowPointerAt(int index)
        {return overflowList.get(index);}

    void pushToOverflowList(long overflowPointer)
        {overflowList.push(overflowPointer);}

    long popOverflowPointer()
        {return(overflowList.pop());}

    void setOverflowPointerAt(int index, long value)
        {overflowList.set(index, value);}

    long removeLastOverflowPointer()
        {return(overflowList.removeLast());}

    long getLastOverflowPointer()
        {return(overflowList.getLast());}

    void addToValueList(int index, String value)
        {valueList.add(index, value);}

    String getValueAt(int index)
        {return valueList.get(index);}

    void pushToValueList(String value)
        {valueList.push(value);}

    String popValue()
        {return valueList.pop();}

    String removeLastValue()
        {return  valueList.removeLast();}

    long getNextPagePointer()
        {return(nextPagePointer);}

    void setNextPagePointer(long next)
        {nextPagePointer = next;}

    long getPrevPagePointer()
        {return prevPagePointer;}

    void setPrevPagePointer(long prevPagePointer) {
        this.prevPagePointer = prevPagePointer;
    }

    String removeEntryAt(int index, BPlusConfiguration conf)
            throws InvalidBTreeStateException {
        keyArray.remove(index);
        overflowList.remove(index);
        String s = valueList.remove(index);
        decrementCapacity(conf);
        return(s);
    }

    /**
     *
     * Leaf node write structure is as follows:
     *
     *  -- node type -- (2 bytes)
     *  -- next pointer -- (8 bytes)
     *  -- prev pointer -- (8 bytes)
     *  -- key/value pairs -- (max size * (key size + satellite size))
     *
     * @param r pointer to *opened* B+ tree file
     * @param conf configuration parameter
     * @throws IOException is thrown when an I/O operation fails
     */
    @Override
    public void writeNode(RandomAccessFile r, BPlusConfiguration conf,
                          BPlusTreePerformanceCounter bPerf)
            throws IOException {

        // update root index in the file
        if(this.isRoot()) {
            r.seek(conf.getHeaderSize()-8L);
            r.writeLong(getPageIndex());
        }

        // account for the header page as well.
        r.seek(getPageIndex());

        // now write the node type
        r.writeShort(getPageType());

        // write the next pointer
        r.writeLong(nextPagePointer);

        // write the prev pointer
        r.writeLong(prevPagePointer);

        // then write the current capacity
        r.writeInt(getCurrentCapacity());

        // now write the Key/Value pairs
        for(int i = 0; i < getCurrentCapacity(); i++) {
            r.writeLong(getKeyAt(i));
            r.writeLong(getOverflowPointerAt(i));
            r.write(valueList.get(i).getBytes(StandardCharsets.UTF_8));
        }

        // annoying correction
        if(r.length() < getPageIndex()+conf.getPageSize())
            {r.setLength(getPageIndex()+conf.getPageSize());}

        bPerf.incrementTotalLeafNodeWrites();
    }

    @Override
    public void printNode() {
        System.out.println("\nPrinting node of type: " + getNodeType().toString() +
                " with index: " + getPageIndex());
        System.out.println("Current node capacity is: " + getCurrentCapacity());

        System.out.println("Next pointer (index): " + getNextPagePointer());
        System.out.println("Prev pointer (index): " + getPrevPagePointer());

        System.out.println("\nPrinting stored (Key, Value, ovf) tuples:");
        for(int i = 0; i < keyArray.size(); i++) {
            System.out.print(" (" +
                    keyArray.get(i).toString() + ", " +
                    valueList.get(i) + ", " +
                    overflowList.get(i) + ") ");
        }
        System.out.println("\n");
    }

}
