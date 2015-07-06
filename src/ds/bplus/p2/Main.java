package ds.bplus.p2;

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
        boolean fastTrials = true;
        boolean recreateTree = true;
        BPlusConfiguration btconf = new BPlusConfiguration();
        BPlusTreePerformanceCounter bPerf = new BPlusTreePerformanceCounter(true);
        BPlusTree bt = new BPlusTree(btconf, recreateTree ? "rw+" : "rw", bPerf);

        long skey = 0;
        long eKey = 100000;
        String val = "1234567890";
        boolean unique = true;
        bt.printCurrentConfiguration();
        if(recreateTree) {
            Utilities.sequentialAddToTree(skey, eKey,
                    val, unique, bt);
            bPerf.printTotalStatistics();
        }

        if(fastTrials)
            {TestRunner.runDefaultTrialsFast(bPerf);}
        else
            {TestRunner.runBench(bPerf);}

        // finally close it.
        bt.commitTree();

    }

}
