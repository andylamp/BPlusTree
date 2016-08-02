package ds.bplus.bptree;

import java.io.IOException;
import java.io.RandomAccessFile;

@SuppressWarnings("unused")
class TreeLookupOverflowNode extends TreeNode {

    private long next; // next pointer

    /**
     * Constructor which takes into the node type as well as the
     * page index
     *
     * @param pageIndex the page index in the file
     */
    TreeLookupOverflowNode(long pageIndex, long nextPointer) {
        super(TreeNodeType.TREE_LOOKUP_OVERFLOW, pageIndex);
        this.next = nextPointer;
    }

    /**
     * Write a lookup page overflow to the page index; the node should
     * have the following structure:
     * <p>
     * -- node type -- (2 bytes)
     * -- next pointer -- (8 bytes)
     * -- current capacity -- (4 bytes)
     * <p>
     * -- page indexes (in place of keys) (8 bytes)
     *
     * @param r     an *already* open pointer which points to our B+ Tree file
     * @param conf  B+ Tree configuration
     * @param bPerf instance of performance counter class
     * @throws IOException is thrown when an I/O operation fails
     */
    @Override
    public void writeNode(RandomAccessFile r,
                          BPlusConfiguration conf,
                          BPlusTreePerformanceCounter bPerf)
            throws IOException {

        // account for the header page as well
        r.seek(getPageIndex());

        // write the node type
        r.writeShort(getPageType());

        // write the next pointer
        r.writeLong(next);

        // write current capacity
        r.writeInt(getCurrentCapacity());

        // now write the index values
        for (int i = 0; i < getCurrentCapacity(); i++) {
            r.writeLong(getKeyAt(i));
        }

    }


    /**
     * Get the next pointer of the node
     *
     * @return the next pointer value
     */
    long getNextPointer() {
        return next;
    }

    /**
     * Set the next pointer of the node
     *
     * @param nextPointer the new next pointer
     */
    public void setNextPointer(long nextPointer) {
        this.next = nextPointer;
    }

    @Override
    public void printNode() {
        System.out.println("\nPrinting node of type: " + getNodeType().toString() +
                " with index: " + getPageIndex());
        System.out.println("Current node capacity is: " + getCurrentCapacity());

        System.out.println("\nPrinting tuples: \n");
        for (Long key : keyArray) {
            System.out.print(key);
        }

        System.out.println("\n");

    }
}
