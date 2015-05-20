package tuc.ds.bptree;

import java.util.LinkedList;

public class RangeResult {

    private LinkedList<KeyValueWrapper> queryResult;

    public RangeResult()
        {this.queryResult = new LinkedList<>();}

    public LinkedList<KeyValueWrapper> getQueryResult()
        {return(queryResult);}
}
