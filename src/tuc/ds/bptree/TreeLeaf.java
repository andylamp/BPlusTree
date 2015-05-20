package tuc.ds.bptree;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;

public class TreeLeaf extends TreeNode {
    private boolean isOverflowPage;
    private long nextPagePointer;
    private long prevPagePointer;
    private LinkedList<String> valueList;


    public TreeLeaf(long nextPagePointer, long prevPagePointer,
                    TreeNodeType nodeType, long pageIndex) {
        super(nodeType, pageIndex);
        if(nodeType == TreeNodeType.TREE_ROOT_LEAF && nextPagePointer > 0)
            {throw new IllegalArgumentException("Can't have leaf " +
                    "root with non-null next pointer");}
        this.isOverflowPage = (nodeType == TreeNodeType.TREE_LEAF_OVERFLOW);
        this.nextPagePointer = nextPagePointer;
        this.prevPagePointer = prevPagePointer;
        this.valueList = new LinkedList<>();
    }

    public void setPairAtPosition(int index, long key, String payload) {
        if(payload == null || key < 0 || !isAllowedEntryIndex(index)) {
            throw new IllegalArgumentException("Can't have " +
                    "index larger than array size or null string");
        }
        // set at position
        //setKeyValuePairAt(index, key, payload);
    }

    public void addToValueList(int index, String value)
        {valueList.add(index, value);}

    public String getValueAt(int index)
        {return valueList.get(index);}

    public void pushToValueList(String value)
        {valueList.push(value);}

    public String popValue()
        {return valueList.pop();}

    public String removeLastValue()
        {return  valueList.removeLast();}

    public boolean isOverflowPage()
        {return(isOverflowPage);}

    public void setOverflowPage(boolean ovfFlag)
        {isOverflowPage = ovfFlag;}

    public long getNextPagePointer()
        {return(nextPagePointer);}

    public void setNextPagePointer(long next)
        {nextPagePointer = next;}

    public long getPrevPagePointer()
        {return prevPagePointer;}

    public void setPrevPagePointer(long prevPagePointer)
        {this.prevPagePointer = prevPagePointer;}

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
     * @throws IOException
     */
    @Override
    void writeNode(RandomAccessFile r, BPlusConfiguration conf)
            throws IOException {

        // update root index in the file
        if(this.isRoot()) {
            r.seek(conf.getHeaderSize()-8L);
            r.writeLong(getPageIndex());
        }

        // account for the header page as well.
        r.seek((getPageIndex()+1)*conf.getPageSize());

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
            r.write(valueList.get(i).getBytes(StandardCharsets.UTF_8));
        }

    }

    @Override
    public void printNode() {
        System.out.println("\nPrinting node of type: " + getNodeType().toString() +
                " with index: " + getPageIndex());
        System.out.println("Current node capacity is: " + getCurrentCapacity());

        System.out.println("Next pointer (index): " + getNextPagePointer());
        System.out.println("Prev pointer (index): " + getPrevPagePointer());

        System.out.println("\nPrinting stored (Key, Value) pairs:");
        for(int i = 0; i < keyArray.size(); i++) {
            System.out.print(" (" +
                    keyArray.get(i).toString() + ", " +
                    valueList.get(i) + ") ");
        }
        System.out.println("\n");
    }

}
