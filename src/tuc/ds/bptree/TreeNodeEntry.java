package tuc.ds.bptree;

public class TreeNodeEntry<T> implements Comparable<TreeNodeEntry<T>> {
    private Long key;       // key
    private T payload;      // payload (either pointers or satellite data)

    public TreeNodeEntry(long key, T payload) {
        this.key = key;
        this.payload = payload;
    }

    public T getPayload()
        {return payload;}

    public Long getKey()
        {return(key);}

    public void setKey(long key)
        {this.key = key;}

    public void setPayload(T payload)
        {this.payload = payload;}

    public void setPair(long key, T payload)
        {this.key = key; this.payload = payload;}

    @Override
    public int compareTo(TreeNodeEntry<T> entry)
        {return(this.key.compareTo(entry.getKey()));}

    public void printPair()
        {System.out.println("Key is: " + key + "\nPayload is: " + payload.toString());}

}
