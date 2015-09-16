package ds.bplus.util;

import ds.bplus.bptree.BPlusTree;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;

public class Utilities {

    public static Random rand = new Random();

    /**
     * Returns a pseudo-random number between min and max, inclusive.
     * The difference between min and max can be at most
     * <code>Integer.MAX_VALUE - 1</code>.
     *
     * @param min Minimum value
     * @param max Maximum value.  Must be greater than min.
     * @return Integer between min and max, inclusive.
     * @see Random#nextInt(int)
     */

    public static int randInt(int min, int max) {
        // nextInt is normally exclusive of the top value,
        // so add 1 to make it inclusive
        return rand.nextInt((max - min) + 1) + min;
    }

    /**
     * Helper to add stuff to the tree
     * @param from key to start
     * @param to key to end
     * @param val value to tie with the keys
     * @param unique allow duplicates?
     * @param bt B+ Tree instance
     * @throws IOException
     */
    public static void sequentialAddToTree(long from, long to, String val,
                                           boolean unique, BPlusTree bt)
            throws IOException, InvalidBTreeStateException {
        for(long i = from; i < to; i++) {
            if(i%10000 == 0){
                System.out.println("Currently at: " + i);
            }
            bt.insertKey(i, val, unique);
        }
    }

    public static void fuzzyAddToTree(long from, long to, String val,
                                      boolean unique, BPlusTree bt)
            throws IOException, InvalidBTreeStateException {

        if(!unique) {
            LinkedList<Long> l = new LinkedList<>();
            for(long i = from; i < to; i++) {
                l.add(i-from);
            }
            int index;

            for(long i = from; i < to; i++) {
                index = rand.nextInt(l.size());
                bt.insertKey(l.remove(index), val, unique);
            }
        }
    }
}
