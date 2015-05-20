package tuc.ds.bptree;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.InvalidPropertiesFormatException;
import java.util.LinkedList;

/**
 *
 * Class that describes all the common properties that
 * each of the node types have.
 *
 */

public abstract class TreeNode {
    private TreeNodeType nodeType;          // actual node type
    private long pageIndex;                 // node page index
    private int currentCapacity;            // current capacity
    protected LinkedList<Long> keyArray;    // key array


    /**
     * Constructor which takes into the node type as well as the
     * page index
     * @param nodeType the actual node type
     * @param pageIndex the page index in the file
     */
    public TreeNode(TreeNodeType nodeType, long pageIndex)  {
        this.nodeType = nodeType;           // actual node type
        this.pageIndex = pageIndex;         // node page index
        this.currentCapacity = 0;           // current capacity
        this.keyArray = new LinkedList<>(); // instantiate the linked list
    }

    /**
     * Check if the node is full (and needs splitting)
     * @param conf configuration to deduce which degree to use
     *
     * @return true is the node is full false if it's not.
     */
    public boolean isFull(BPlusConfiguration conf) {
        if(isLeaf())
            {return(conf.getMaxLeafNodeCapacity() == currentCapacity);}
        else
            // internal
            {return(conf.getMaxInternalNodeCapacity() == currentCapacity);}
    }

    public int getCurrentCapacity()
        {return(currentCapacity);}

    public void incrementCapacity()
        {currentCapacity++;}

    public void decrementCapacity()
        {currentCapacity--;}

    public void setCurrentCapacity(int newCap)
        {currentCapacity = newCap;}

    /**
     * Check if the node is empty (and *definitely* needs merging)
     *
     * @return true if it is empty false if it's not.
     */
    public boolean isEmpty()
        {return(currentCapacity == 0);}

    /**
     * Check if the node in question is a leaf (including root)
     *
     * @return true if the node is a leaf, false if it's not.
     */
    public boolean isLeaf() {
        return(nodeType == TreeNodeType.TREE_LEAF ||
                nodeType == TreeNodeType.TREE_LEAF_OVERFLOW ||
                nodeType == TreeNodeType.TREE_ROOT_LEAF);
    }

    /**
     * Check if the node in question is a tree root.
     *
     * @return true if it is a tree root, false if it's not.
     */
    public boolean isRoot() {
        return(nodeType == TreeNodeType.TREE_ROOT_INTERNAL ||
                nodeType == TreeNodeType.TREE_ROOT_LEAF);
    }

    /**
     * Check if the node in question is an internal node (including root)
     *
     * @return true if the node is an internal node, false if it's not.
     */
    public boolean isInternalNode() {
        return(nodeType == TreeNodeType.TREE_INTERNAL_NODE ||
                nodeType == TreeNodeType.TREE_ROOT_INTERNAL);
    }

    /**
     * Explicitly set the node type
     *
     * @param nodeType set the node type
     */
    public void setNodeType(TreeNodeType nodeType) {
        // check if we presently are a leaf
        if(isLeaf()) {
            this.nodeType = nodeType;
            if(isInternalNode())
                {throw new IllegalArgumentException("Cannot convert Leaf to Internal Node");}
        }
        // it must be an internal node
        else {
            this.nodeType = nodeType;
            if(isLeaf())
                {throw new IllegalArgumentException("Cannot convert Internal Node to Leaf");}
        }
    }

    /**
     * Return the node type
     *
     * @return the current node type
     */
    public TreeNodeType getNodeType()
        {return(nodeType);}

    public long getKeyAt(int index)
        {return(keyArray.get(index));}

    /**
     * Return the page index
     *
     * @return current page index
     */
    public long getPageIndex()
        {return pageIndex;}

    /**
     * Update the page index
     *
     * @param pageIndex new page index
     */
    public void setPageIndex(long pageIndex)
        {this.pageIndex = pageIndex;}

    /**
     * Set the key in the array at specific position
     *
     * @param index index to set the key
     * @param key key to set in position
     */
    public void setKeyArrayAt(int index, long key)
        {keyArray.set(index, key);}

    /**
     * Add key at index while shifting entries
     * pointed by index and after by one.
     *
     * @param index index to shift keys and add
     * @param key key to add in position
     */
    public void addToKeyArrayAt(int index, long key)
        {keyArray.add(index, key);}

    /**
     * Push a key to head of the array
     *
     * @param key key to push
     */
    public void pushToKeyArray(long key)
        {keyArray.push(key);}

    /**
     * Pop the key at the head of the array
     *
     * @return key that is in the head of the array
     */
    public long popKey()
        {return keyArray.pop();}

    /**
     * Remove and pop the last key of the array
     *
     * @return key that is in the last place of the array
     */
    public long removeLastKey()
        {return keyArray.removeLast();}

    /**
     * Remove and pop the key at specific position
     *
     * @param index index that points where to remvoe the key
     * @return removed key
     */
    public long removeKeyAt(int index)
        {return(keyArray.remove(index));}

    /**
     * Get the page type that maps the enumeration to numbers that are
     * easily stored in our file.
     *
     * @return the number representation of the node type
     * @throws InvalidPropertiesFormatException
     */
    protected short getPageType()
            throws InvalidPropertiesFormatException {
        switch(getNodeType()) {
            case TREE_LEAF:             // LEAF
                {return(1);}

            case TREE_INTERNAL_NODE:    // INTERNAL NODE
                {return(2);}

            case TREE_ROOT_INTERNAL:    // INTERNAL NODE /w ROOT
                {return(3);}

            case TREE_ROOT_LEAF:        // LEAF NODE /w ROOT
                {return(4);}

            case TREE_LEAF_OVERFLOW:    // LEAF OVERFLOW NODE
                {return(5);}

            default: {
                throw new InvalidPropertiesFormatException("Unknown " +
                        "node value read; file possibly corrupt?");
            }
        }
    }

    /**
     * Abstract method that all classes must implement that writes
     * each node type to a page slot.
     *
     * More details in each implementation.
     *
     * @param r *already* open pointer which points to our B+ Tree file
     * @param conf B+ Tree configuration
     * @throws IOException
     */
    public abstract void writeNode(RandomAccessFile r, BPlusConfiguration conf)
            throws IOException;

    /**
     *
     * Each class must implement it's own printing method.
     *
     */
    public abstract void printNode();

}
