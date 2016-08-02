package ds.bplus.bptree;

import ds.bplus.util.InvalidBTreeStateException;

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
@SuppressWarnings("unused")
abstract class TreeNode {
    final LinkedList<Long> keyArray;  // key array
    private TreeNodeType nodeType;    // actual node type
    private long pageIndex;           // node page index
    private int currentCapacity;      // current capacity
    private boolean beingDeleted;     // deleted flag


    /**
     * Constructor which takes into the node type as well as the
     * page index
     * @param nodeType the actual node type
     * @param pageIndex the page index in the file
     */
    TreeNode(TreeNodeType nodeType, long pageIndex) {
        this.nodeType = nodeType;           // actual node type
        this.pageIndex = pageIndex;         // node page index
        this.currentCapacity = 0;           // current capacity
        this.keyArray = new LinkedList<>(); // instantiate the linked list
        this.beingDeleted = true;
    }

    /**
     * Check if the node is full (and needs splitting)
     * @param conf configuration to deduce which degree to use
     *
     * @return true is the node is full false if it's not.
     */
    boolean isFull(BPlusConfiguration conf) {
        if(isLeaf()) {
            return(isOverflow() ?
                    (conf.getMaxOverflowNodeCapacity() == currentCapacity) :
                    (conf.getMaxLeafNodeCapacity() == currentCapacity));}
        else
            // internal
            {return(conf.getMaxInternalNodeCapacity() == currentCapacity);}
    }

    /**
     * Check if the node is underutilized and needs to be merged
     *
     * @param conf B+ Tree configuration reference
     * @return true is the node needs to be merged or false if it's not
     */
    boolean isTimeToMerge(BPlusConfiguration conf) {
        // for roots (internal or leaf) return true only when empty
        if(isRoot())
            {return(getCurrentCapacity() <= 1);}
        else if(isLeaf()) {
            // for overflow pages return true only if empty
            if (isOverflow())
                {return (isEmpty());}
            // otherwise return based on degree
            else
                {return (conf.getMinLeafNodeCapacity() >= currentCapacity);}
        } else // internal
        {
            return (conf.getMinInternalNodeCapacity() >= currentCapacity);
        }
    }

    /**
     * Returns the current node capacity
     *
     * @return the newCap variable value.
     */
    int getCurrentCapacity() {
        return (currentCapacity);
    }

    /**
     * Set the current capacity
     *
     * @param newCap replace node capacity with this argument.
     */
    void setCurrentCapacity(int newCap) {
        currentCapacity = newCap;
    }

    /**
     * Increment the node capacity by one.
     *
     * @param conf configuration instance for validating the limits.
     * @throws InvalidBTreeStateException is thrown when the capacity limits are violated after incrementing.
     */
    void incrementCapacity(BPlusConfiguration conf) throws InvalidBTreeStateException {
        currentCapacity++;
        validateNodeCapacityLimits(conf);
    }

    /**
     * Decrement the node capacity by one.
     *
     * @param conf configuration instance for validating the limits.
     * @throws InvalidBTreeStateException is thrown when the capacity limits are violated after decrementing.
     */
    void decrementCapacity(BPlusConfiguration conf)
            throws InvalidBTreeStateException {
        currentCapacity--;
        validateNodeCapacityLimits(conf);
    }

    /**
     * Function that validates the node capacity invariants based on the current configuration instance.
     *
     * @param conf configuration instance for validating the limits.
     * @throws InvalidBTreeStateException is thrown when the capacity limits are violated upon checking.
     */
    private void validateNodeCapacityLimits(BPlusConfiguration conf)
            throws InvalidBTreeStateException {

        if(isRoot()) {
            if(currentCapacity < 0) {
                throw new InvalidBTreeStateException("Cannot have less than zero elements");
            } else if(isLeaf() && currentCapacity > conf.getMaxLeafNodeCapacity()) {
                throw new InvalidBTreeStateException("Exceeded leaf node " +
                        "allowed capacity at root");
            } else if(isInternalNode() && currentCapacity > conf.getMaxInternalNodeCapacity()) {
                throw new InvalidBTreeStateException("Exceeded internal node " +
                        "allowed capacity at root");
            }
        } else {
            if (isLookupPageOverflowNode()) {
                if (beingDeleted && currentCapacity < 0) {
                    throw new InvalidBTreeStateException("Cannot have less than " +
                            0 + " elements in a lookup overflow node when deleting it");
                } else if (currentCapacity > conf.getMaxLookupPageOverflowCapacity()) {
                    throw new InvalidBTreeStateException("Exceeded lookup overflow node " +
                            "allowed capacity (node)");
                }
            }
            if(isOverflow()) {
                if(beingDeleted && currentCapacity < 0) {
                    throw new InvalidBTreeStateException("Cannot have less than " +
                            0 + " elements in a overflow node when deleting it");
                }
                else if(currentCapacity > conf.getMaxOverflowNodeCapacity()) {
                    throw new InvalidBTreeStateException("Exceeded overflow node " +
                            "allowed capacity (node)");
                }
            }
            else if(isLeaf()) {
                if(beingDeleted && currentCapacity < 0) {
                    throw new InvalidBTreeStateException("Cannot have less than " +
                            0 + " elements in a leaf node when deleting it");
                } else if(!beingDeleted && currentCapacity < conf.getMinLeafNodeCapacity()) {
                    throw new InvalidBTreeStateException("Cannot have less than " +
                            conf.getMinLeafNodeCapacity() + " elements in a leaf node");
                }
                else if(currentCapacity > conf.getMaxLeafNodeCapacity()) {
                    throw new InvalidBTreeStateException("Exceeded leaf node " +
                            "allowed capacity (node)");
                }
            } else if(isInternalNode()) {
                if(beingDeleted && currentCapacity < 0) {
                    throw new InvalidBTreeStateException("Cannot have less than " +
                            0 + " elements in an internal node");
                }
                else if(!beingDeleted && currentCapacity < conf.getMinInternalNodeCapacity()) {
                    throw new InvalidBTreeStateException("Cannot have less than " +
                            conf.getMinInternalNodeCapacity() +
                            " elements in an internal node");
                }
                else if(currentCapacity > conf.getMaxInternalNodeCapacity()) {
                    throw new InvalidBTreeStateException("Exceeded internal node " +
                            "allowed capacity (node)");
                }
            }
        }
    }

    /**
     * Being deleted flag
     *
     * @return true if the node is marked to be deleted, false otherwise.
     */
    public boolean getBeingDeleted() {
        return beingDeleted;
    }

    /**
     * Set being deleted flag
     *
     * @param beingDeleted value to set the flag.
     */
    void setBeingDeleted(boolean beingDeleted) {
        this.beingDeleted = beingDeleted;
    }

    /**
     * Check if the node is empty (and *definitely* needs merging)
     *
     * @return true if it is empty false if it's not.
     */
    boolean isEmpty()
        {return(currentCapacity == 0);}

    /**
     * Check if the node in question is an overflow page
     *
     * @return true if the node is an overflow page, false if it's not
     */
    boolean isOverflow() {
        return (nodeType == TreeNodeType.TREE_LEAF_OVERFLOW);
    }

    /**
     * Check if the node in question is a leaf (including root)
     *
     * @return true if the node is a leaf, false if it's not.
     */
    boolean isLeaf() {
        return(nodeType == TreeNodeType.TREE_LEAF ||
                nodeType == TreeNodeType.TREE_LEAF_OVERFLOW ||
                nodeType == TreeNodeType.TREE_ROOT_LEAF);
    }

    /**
     * Check if the node in question is a tree root.
     *
     * @return true if it is a tree root, false if it's not.
     */
    boolean isRoot() {
        return(nodeType == TreeNodeType.TREE_ROOT_INTERNAL ||
                nodeType == TreeNodeType.TREE_ROOT_LEAF);
    }

    /**
     * Check if the node in question is an internal node (including root)
     *
     * @return true if the node is an internal node, false if it's not.
     */
    boolean isInternalNode() {
        return(nodeType == TreeNodeType.TREE_INTERNAL_NODE ||
                nodeType == TreeNodeType.TREE_ROOT_INTERNAL);
    }

    /**
     * Check if the node in question is a lookup page overflow node
     *
     * @return true if the node is a lookup page overflow node, false otherwise
     */
    boolean isLookupPageOverflowNode() {
        return (nodeType == TreeNodeType.TREE_LOOKUP_OVERFLOW);
    }

    /**
     * Return the node type
     *
     * @return the current node type
     */
    TreeNodeType getNodeType() {
        return (nodeType);
    }

    /**
     * Explicitly set the node type
     *
     * @param nodeType set the node type
     */
    void setNodeType(TreeNodeType nodeType) {
        // check if we presently are a leaf
        if (isLeaf()) {
            this.nodeType = nodeType;
            if (isInternalNode()) {
                throw new IllegalArgumentException("Cannot convert Leaf to Internal Node");
            }
        }
        // it must be an internal node
        else {
            this.nodeType = nodeType;
            if (isLeaf()) {
                throw new IllegalArgumentException("Cannot convert Internal Node to Leaf");
            }
        }
    }

    /**
     * Get the specific key at position indicated by <code>index</code>
     * @param index the position to get the key
     * @return the key at position
     */
    long getKeyAt(int index)
        {return(keyArray.get(index));}

    /**
     * Return the page index
     *
     * @return current page index
     */
    long getPageIndex()
        {return pageIndex;}

    /**
     * Update the page index
     *
     * @param pageIndex new page index
     */
    void setPageIndex(long pageIndex)
        {this.pageIndex = pageIndex;}

    /**
     * Set the key in the array at specific position
     *
     * @param index index to set the key
     * @param key key to set in position
     */
    void setKeyArrayAt(int index, long key)
        {keyArray.set(index, key);}

    /**
     * Add key at index while shifting entries
     * pointed by index and after by one.
     *
     * @param index index to shift keys and add
     * @param key key to add in position
     */
    void addToKeyArrayAt(int index, long key)
        {keyArray.add(index, key);}

    /**
     * Push a key to head of the array
     *
     * @param key key to push
     */
    void pushToKeyArray(long key)
        {keyArray.push(key);}

    /**
     * Add a key to the last place of the array
     *
     * @param key key to add
     */
    void addLastToKeyArray(long key)
        {keyArray.addLast(key);}

    /**
     * Get last element
     *
     * @return return the last key
     */
    long getLastKey()
        {return keyArray.getLast();}

    /**
     * Get first key
     *
     * @return return the first key value
     */
    long getFirstKey()
        {return keyArray.getFirst();}

    /**
     * Pop the key at the head of the array
     *
     * @return key that is in the head of the array
     */
    long popKey()
        {return keyArray.pop();}

    /**
     * Remove and pop the last key of the array
     *
     * @return key that is in the last place of the array
     */
    long removeLastKey()
        {return keyArray.removeLast();}

    /**
     * Remove and pop the key at specific position
     *
     * @param index index that points where to remvoe the key
     * @return removed key
     */
    long removeKeyAt(int index)
        {return(keyArray.remove(index));}

    /**
     * Get the page type that maps the enumeration to numbers that are
     * easily stored in our file.
     *
     * @return the number representation of the node type
     * @throws InvalidPropertiesFormatException is thrown when the page type is not matched.
     */
    short getPageType()
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

            case TREE_LOOKUP_OVERFLOW:  // TREE LOOKUP OVERFLOW
            {
                return (6);
            }

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
     * @param r an *already* open pointer which points to our B+ Tree file
     * @param conf B+ Tree configuration
     * @throws IOException is thrown when an I/O operation fails.
     */
    public abstract void writeNode(RandomAccessFile r, BPlusConfiguration conf,
                                   BPlusTreePerformanceCounter bPerf)
            throws IOException;

    /**
     *
     * Each class must implement it's own printing method.
     *
     */
    public abstract void printNode();

}
