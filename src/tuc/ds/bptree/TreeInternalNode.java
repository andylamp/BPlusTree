package tuc.ds.bptree;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;

public class TreeInternalNode extends TreeNode {

    private LinkedList<Long> pointerArray;

    public TreeInternalNode(TreeNodeType nodeType, long pageIndex) {
        super(nodeType, pageIndex);
        pointerArray = new LinkedList<>();
    }


    public void setKeyPointerPairAtPosition(int index, long key, long pointer) {
        if(pointer < -1 || key < 0 || !isAllowedEntryIndex(index)) {
            throw new IllegalArgumentException("Can't have index larger " +
                    "than the array size or pointer less than -1");
        }
        // set at position
        //setKeyValuePairAt(index, key, pointer);
    }

    public long getPointerAt(int index)
        {return(pointerArray.get(index));}

    public long popPointer()
        {return(pointerArray.pop());}

    public long removeLastPointer()
        {return(pointerArray.removeLast());}

    public void addPointerAt(int index, long val)
        {pointerArray.add(index, val);}

    public void setPointerAt(int index, long val)
        {pointerArray.set(index, val);}

    public int getPointerListSize()
        {return(pointerArray.size());}


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
    }

    @Override
    void printNode() {
        System.out.println("\nPrinting node of type: " +
                getNodeType().toString() + " with index: " +
                getPageIndex());

        System.out.println("Current node capacity is: " + getCurrentCapacity());

        System.out.println("\nPrinting stored Keys:");
        for(Long i : keyArray)
            {System.out.print("\t" + i.toString() + " ");}
        System.out.println("\nPrinting stored Pointers");
        for(Long i : pointerArray)
            {System.out.print(" " + i.toString() + " ");}
        System.out.println();
    }


}
