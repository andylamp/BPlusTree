package tuc.ds.bptree;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;

/**
 * Class that is responsible for handling the overflow blocks.
 *
 * Although it is derived from the TreeNode class we *don't* use
 * the key array at all (this could be improved but... well...)
 */
public class TreeOverflow extends TreeNode {


    private long nextPagePointer;
    private long prevPagePointer;
    private LinkedList<String> valueList;

    /**
     * Constructor which takes into the node type as well as the
     * page index
     *
     * @param pageIndex the page index in the file
     */
    public TreeOverflow(long nextPagePointer, long prevPagePointer,
                        long pageIndex) {
        super(TreeNodeType.TREE_LEAF_OVERFLOW, pageIndex);
        valueList = new LinkedList<>();
        this.nextPagePointer = nextPagePointer;
        this.prevPagePointer = prevPagePointer;
    }

    public void pushToValueList(String value)
        {valueList.push(value);}

    public void addToValueList(int index, String value)
        {valueList.add(index, value);}

    public String getValueAt(int index)
        {return valueList.get(index);}

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
     * Overflow node write structure is as follows:
     *
     *  -- node type -- (2 bytes)
     *  -- next pointer -- (8 bytes)
     *  -- prev pointer -- (8 bytes)
     *  -- values -- (max size * satellite size)
     *
     * @param r pointer to *opened* B+ tree file
     * @throws IOException
     */
    @Override
    public void writeNode(RandomAccessFile r, BPlusConfiguration conf,
                          BPlusTreePerformanceCounter bPerf)
            throws IOException {
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

        // now write the values
        for(int i = 0; i < getCurrentCapacity(); i++)
            {r.write(valueList.get(i).getBytes(StandardCharsets.UTF_8));}

        // annoying correction
        if(r.length() < getPageIndex()+conf.getPageSize())
            {r.setLength(getPageIndex()+conf.getPageSize());}

        bPerf.incrementTotalOverflowNodeWrites();
    }

    @Override
    public void printNode() {
        System.out.println("\nPrinting node of type: " + getNodeType().toString() +
                " with index: " + getPageIndex());
        System.out.println("Current node capacity is: " + getCurrentCapacity());

        System.out.println("Next pointer (index): " + getNextPagePointer());
        System.out.println("Prev pointer (index): " + getPrevPagePointer());

        System.out.println("\nPrinting stored values:");
        for(int i = 0; i < keyArray.size(); i++) {
            System.out.print(" " + valueList.get(i) + " ");
        }
        System.out.println("\n");
    }
}
