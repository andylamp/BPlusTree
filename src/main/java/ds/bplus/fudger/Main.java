package ds.bplus.fudger;

import ds.bplus.bptree.BPlusConfiguration;
import ds.bplus.bptree.BPlusTree;
import ds.bplus.bptree.BPlusTreePerformanceCounter;
import ds.bplus.bptree.SearchResult;
import ds.bplus.util.InvalidBTreeStateException;
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

        int tlen = 10000;
        long skey = 0;
        long eKey = tlen;
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
            for(int i = tlen-1; i > 0; i--) {
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
            // we need to fix this
            LinkedList<Integer> l = new LinkedList<>(),
                                lq = new LinkedList<>();
            for(int i = 0; i < tlen; i++) {
                l.add(i);
            }
            int index;
            int found_cnt = 0;
            Random r = new Random();
            for(int i = 0; i < tlen; i++) {
                index = r.nextInt(l.size());
                lq.push(l.remove(index));
                System.out.println(" -- Iteration " + i);
                bt.deleteKey(lq.getFirst(), true);
            }




            System.out.println("Total pages in the before second fill-up: " + bt.getTotalTreePages());



            Utilities.sequentialAddToTree(skey, eKey,
                    val, unique, bt);

            for(int i = 0; i < tlen; i++) {
                l.add(i);
            }

            for(int i = 0; i < tlen/2; i++) {
                index = r.nextInt(l.size());
                bt.deleteKey(l.remove(index), true);
            }

            for(int i = 0; i < tlen/2; i++) {
                index = r.nextInt(l.size());
                SearchResult sres = bt.searchKey(l.remove(index), false);
                if(sres.isFound()) {
                    found_cnt++;
                    System.out.println(" -- Key " + sres.getKey() + " is found");
                } else {
                    System.out.println(" -- Key " + sres.getKey() + " is NOT found");
                }
            }
            System.out.println("Found keys: " + found_cnt + " out of: " + tlen/2 + " keys");

            //int p = 0;
        } else if(flag == 7) {
            LinkedList<Integer> l = new LinkedList<>();
            int index;
            int found_cnt = 0;
            Random r = new Random();

            for(int i = 0; i < tlen; i++) {l.add(i);}

            for(int i = 0; i < tlen; i++) {
                index = r.nextInt(l.size());
                SearchResult sres = bt.searchKey(l.remove(index), false);
                if(sres.isFound()) {
                    found_cnt++;
                    System.out.println(" -- Key " + sres.getKey() + " is found");
                } else {
                    System.out.println(" -- Key " + sres.getKey() + " is NOT found");
                }
            }
            System.out.println("Found keys: " + found_cnt + " out of: " + tlen + " keys");
        } else if(flag == 8) {
            LinkedList<Integer> l = new LinkedList<>();
            int index;
            int found_cnt = 0;


            l.add(24);
            l.add(70);
            l.add(13);
            l.add(6);
            l.add(23);
            l.add(60);
            l.add(50);
            l.add(43); // should have a problem there
            l.add(51);
            l.add(66);
            l.add(76); // exception

            int lsize = l.size();
            for(int i = 0; i < lsize; i++) {
                bt.deleteKey(l.removeFirst(), true);
            }
        }



        //TrialsClass.runSearchTrial(200, 0, 99, true, bPerf, true);

        /*
        if(fastTrials)
            {TestRunner.runDefaultTrialsFast(bPerf);}
        else
            {TestRunner.runBench(bPerf);}
        */
        System.out.println("Total pages in the end: " + bt.getTotalTreePages());
        // finally close it.
        bt.commitTree();

    }

}
