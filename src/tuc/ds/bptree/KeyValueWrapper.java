package tuc.ds.bptree;

public class KeyValueWrapper {

    private long key;
    private String value;

    public long getKey()
        {return key;}

    public String getValue()
        {return value;}

    public KeyValueWrapper(long key, String value) {
        this.key = key;
        this.value = value;
    }


}
