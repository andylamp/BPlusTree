package ds.bplus.fudger;

import ds.bplus.bptree.BPlusConfiguration;
import ds.bplus.bptree.BPlusTree;
import ds.bplus.bptree.BPlusTreePerformanceCounter;
import ds.bplus.util.InvalidBTreeStateException;
import ds.bplus.util.TestRunner;
import ds.bplus.util.Utilities;

import java.io.IOException;

public class Main {

    public static void main(String[] args)
            throws IOException, InvalidBTreeStateException {
        boolean fastTrials = false;
        boolean recreateTree = true;
        BPlusConfiguration btconf = new BPlusConfiguration();
        BPlusTreePerformanceCounter bPerf = new BPlusTreePerformanceCounter(true);
        BPlusTree bt = new BPlusTree(btconf, recreateTree ? "rw+" : "rw", bPerf);

        long skey = 0;
        long eKey = 6;
        String val = "1234567890";
        boolean unique = true;
        bt.printCurrentConfiguration();
        if(recreateTree) {
            Utilities.sequentialAddToTree(skey, eKey,
                    val, unique, bt);
            bPerf.printTotalStatistics();
        }
        /*
        for(int i = 0; i < 150; i++)
            {bt.insertKey(100, "1234567890", false);}

        for(int i = 0; i < 100; i++)
            {bt.deleteKey(100, true);}
        */

        for(int i = 5; i >= 0; i--) {
            bt.deleteKey(i, true);
        }
        if(fastTrials)
            {TestRunner.runDefaultTrialsFast(bPerf);}
        else
            {TestRunner.runBench(bPerf);}
        
        // finally close it.
        bt.commitTree();

    }

}
