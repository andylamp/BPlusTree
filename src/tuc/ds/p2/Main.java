package tuc.ds.p2;

import tuc.ds.bptree.BPlusConfiguration;
import tuc.ds.bptree.BPlusTree;
import tuc.ds.bptree.BPlusTreePerformanceCounter;
import tuc.ds.util.TestRunner;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        boolean fastTrials = true;
        BPlusConfiguration btconf = new BPlusConfiguration();
        BPlusTreePerformanceCounter bPerf = new BPlusTreePerformanceCounter(true);
        BPlusTree bt = new BPlusTree(btconf, "rw+", bPerf);

        bt.printCurrentConfiguration();
        if(fastTrials) {
            TestRunner.runDefaultTrialsFast(bPerf);
        } else {
            bPerf.printTotalStatistics();
            TestRunner.runBench(bPerf);
        }
        // finally close it.
        bt.commitTree();

    }

}
