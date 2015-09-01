package ds.bplus.fudger;

import ds.bplus.bptree.BPlusConfiguration;
import ds.bplus.bptree.BPlusTree;
import ds.bplus.bptree.BPlusTreePerformanceCounter;
import ds.bplus.bptree.SearchResult;
import ds.bplus.util.InvalidBTreeStateException;
import ds.bplus.util.TestRunner;
import ds.bplus.util.Utilities;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;

public class Main {

    public static void main(String[] args)
            throws IOException, InvalidBTreeStateException {
        boolean fastTrials = false;
        boolean recreateTree = true;
        BPlusConfiguration btconf = new BPlusConfiguration();
        BPlusTreePerformanceCounter bPerf = new BPlusTreePerformanceCounter(true);
        BPlusTree bt = new BPlusTree(btconf, recreateTree ? "rw+" : "rw", bPerf);

        long skey = 0;
        long eKey = 100;
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



        int flag = 6;

        if(flag == 1) {
            for(int i = 99; i > 0; i--) {
                bt.deleteKey(i, true);
            }
        } else if(flag == 2) {
            for(int i = 0; i < 100; i++) {
                bt.deleteKey(i, true);
            }
        }
        else if(flag == 3) {
            LinkedList<Integer> l = new LinkedList<>();
            int min = 70;
            int max = 84;
            for(int i = min; i < max; i++) {
                l.add(i);
            }
            int index;
            Random r = new Random();
            bt.deleteKey(82, true);

            for(int i = min; i < max-1; i++) {
                index = r.nextInt(l.size());
                bt.deleteKey(l.remove(index), true);
                //bt.deleteKey(l.removeFirst(), true);
            }
        } else if(flag == 4) {
            LinkedList<Integer> l = new LinkedList<>();
            l.add(82);
            l.add(81);

            l.add(73);
            /*
            l.add(74);
            l.add(71);
            l.add(79);
            l.add(76);
            */
            //l.add(83);

            int lsize = l.size();
            for(int i = 0; i < lsize; i++) {
                bt.deleteKey(l.removeFirst(), true);
            }
            //SearchResult r = bt.searchKey(83, false);
            //int f = 0;
        }
        else if(flag == 5) {
            LinkedList<Integer> l = new LinkedList<>();

            l.add(82);
            l.add(79);
            l.add(77);
            l.add(70);
            l.add(72);
            l.add(73);
            l.add(76);
            l.add(78); // should have a problem there
            l.add(81);
            l.add(71);
            l.add(74); // exception

            int lsize = l.size();
            for(int i = 0; i < lsize; i++) {
                bt.deleteKey(l.removeFirst(), true);
            }
        }
        else if(flag == 6) {
            LinkedList<Integer> l = new LinkedList<>();
            for(int i = 0; i < 100; i++) {
                l.add(i);
            }
            int index;
            Random r = new Random();
            for(int i = 0; i < 100; i++) {
                index = r.nextInt(l.size());
                bt.deleteKey(l.remove(index), true);
            }

            Utilities.sequentialAddToTree(skey, eKey,
                    val, unique, bt);

            for(int i = 0; i < 100; i++) {
                l.add(i);
            }

            for(int i = 0; i < 50; i++) {
                index = r.nextInt(l.size());
                bt.deleteKey(l.remove(index), true);
            }

            for(int i = 0; i < 50; i++) {
                index = r.nextInt(l.size());
                SearchResult sres = bt.searchKey(l.remove(index), false);
                if(sres.isFound()) {
                    System.out.println(" -- Key " + sres.getKey() + " is found");
                } else {
                    System.out.println(" -- Key " + sres.getKey() + " is NOT found");
                }
            }
        }



        //TrialsClass.runSearchTrial(200, 0, 99, true, bPerf, true);

        if(fastTrials)
            {TestRunner.runDefaultTrialsFast(bPerf);}
        else
            {TestRunner.runBench(bPerf);}

        // finally close it.
        bt.commitTree();

    }

}
