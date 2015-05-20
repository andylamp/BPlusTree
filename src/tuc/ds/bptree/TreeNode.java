package tuc.ds.bptree;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.InvalidPropertiesFormatException;
import java.util.LinkedList;

public abstract class TreeNode {
    private TreeNodeType nodeType;
    private long pageIndex;
    private int currentCapacity;
    protected LinkedList<Long> keyArray;


    public TreeNode(TreeNodeType nodeType, long pageIndex)  {
        this.nodeType = nodeType;
        this.pageIndex = pageIndex;
        this.currentCapacity = 0;
        this.keyArray = new LinkedList<>();
    }

    public boolean isFull(BPlusConfiguration conf) {
        if(nodeType == TreeNodeType.TREE_LEAF ||
                nodeType == TreeNodeType.TREE_LEAF_OVERFLOW ||
                nodeType == TreeNodeType.TREE_ROOT_LEAF) {
            return(conf.getMaxLeafNodeCapacity() == currentCapacity);
        } else { // internal
            return(conf.getMaxInternalNodeCapacity() == currentCapacity);
        }
    }

    public boolean needsSplit(BPlusConfiguration conf) {
        if(nodeType == TreeNodeType.TREE_LEAF ||
                nodeType == TreeNodeType.TREE_LEAF_OVERFLOW ||
                nodeType == TreeNodeType.TREE_ROOT_LEAF) {
            return(conf.getMaxLeafNodeCapacity() == currentCapacity);
        } else { // internal
            return(conf.getMaxInternalNodeCapacity() == currentCapacity);
        }
    }

    public int getCurrentCapacity()
        {return(currentCapacity);}

    public void incrementCapacity()
        {currentCapacity++;}

    public void decrementCapacity()
        {currentCapacity--;}

    public void setCurrentCapacity(int newCap)
        {currentCapacity = newCap;}

    public boolean isEmpty()
        {return(currentCapacity == 0);}

    public boolean isLeaf() {
        return(nodeType == TreeNodeType.TREE_LEAF ||
                nodeType == TreeNodeType.TREE_LEAF_OVERFLOW ||
                nodeType == TreeNodeType.TREE_ROOT_LEAF);
    }

    public boolean isRoot() {
        return(nodeType == TreeNodeType.TREE_ROOT_INTERNAL ||
                nodeType == TreeNodeType.TREE_ROOT_LEAF);
    }

    public boolean isInternalNode() {
        return(nodeType == TreeNodeType.TREE_INTERNAL_NODE ||
                nodeType == TreeNodeType.TREE_ROOT_INTERNAL);
    }

    public void setNodeType(TreeNodeType nodeType)
        {this.nodeType = nodeType;}

    public TreeNodeType getNodeType()
        {return(nodeType);}

    public long getKeyAt(int index)
        {return(keyArray.get(index));}

    public long getPageIndex()
        {return pageIndex;}

    public void setKeyArrayAt(int index, long key)
        {keyArray.set(index, key);}

    public void addToKeyArrayAt(int index, long key)
        {keyArray.add(index, key);}

    public void pushToKeyArray(long key)
        {keyArray.push(key);}

    public void setPageIndex(long pageIndex)
        {this.pageIndex = pageIndex;}

    public long popKey()
        {return keyArray.pop();}

    public long removeLastKey()
        {return keyArray.removeLast();}

    public long removeKeyAt(int index)
        {return(keyArray.remove(index));}

    protected boolean isAllowedEntryIndex(int index)
        {return(keyArray.size() > index && index >= 0/* && index < currentCapacity*/);}

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

    abstract void writeNode(RandomAccessFile r, BPlusConfiguration conf)
            throws IOException;

    abstract void printNode();

}
