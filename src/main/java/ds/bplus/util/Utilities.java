package ds.bplus.util;

import ds.bplus.bptree.BPlusTree;

import java.io.*;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Random;

public class Utilities {

    private static final Random rand = new Random();

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

    static int randInt(int min, int max) {
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
     * @throws IOException is thrown when an I/O operation fails
     */
    public static void sequentialAddToTree(long from, long to, String val,
                                           boolean unique, BPlusTree bt)
            throws IOException, InvalidBTreeStateException {
        long div = (to - from) / 10;
        for(long i = from; i < to; i++) {
            if (i % div == 0) {
                System.out.println("Currently at: " + ((double) i / to) * 100 + " %");
            }
            bt.insertKey(i, val, unique);
        }
        System.out.println("Done!\n");
    }

    /**
     * Add a random sequence of numbers in the tree using unique
     * or discrete values for the key.
     *
     * @param from starting range (>= 0)
     * @param to ending range
     * @param unique use unique values flag
     * @param bt tree instance to add the values
     * @return the list of the values in reverse order of insertion
     * @throws IOException is thrown when an I/O operation fails
     * @throws InvalidBTreeStateException is thrown when there are inconsistencies in the blocks.
     */
    public static LinkedList<Long> fuzzyAddToTree(int from, int to,
                                      boolean unique, BPlusTree bt)
            throws IOException, InvalidBTreeStateException {

        if(from < 0 || to < from)
            {throw new IllegalArgumentException("range must > 0 and from > to");}

        LinkedList<Long> l = new LinkedList<>();
        if(!unique) {
            for(long i = from; i < to; i++) {
                l.push((long) randInt(from, to));
                bt.insertKey(l.peekFirst(), l.peekFirst().toString(), false);
            }
            //writeObjectToFile(l, "lfileex.ser");
        } else {
            //throw new InvalidBTreeStateException("Not yet implemented");
            for(long i = from; i < to; i++)
                {l.add(i);}

            // randomize
            Collections.shuffle(l);

            // add them
            for (Long key : l)
                {bt.insertKey(key, key.toString(), true);}

        }

        return(l);
    }

    /**
     * Add values to a B+ Tree from a file
     *
     * @param filename file to load
     * @param unique unique values?
     * @param bt tree to add the values
     * @return the list of the values in order of insertion
     * @throws IOException is thrown when an I/O operation fails
     * @throws InvalidBTreeStateException is thrown when there are inconsistencies in the blocks.
     * @throws ClassNotFoundException is thrown when the reflection is not able to find the correct class.
     */
    @SuppressWarnings("unused")
    public static LinkedList<Long> addToTreeFromList(String filename, boolean unique,
                                                     BPlusTree bt)
            throws IOException, InvalidBTreeStateException, ClassNotFoundException {

        LinkedList<Long> l = loadListFromFile(filename);
        for (Long key : l)
            {bt.insertKey(key, key.toString(), unique);}
        return(l);
    }

    /**
     * Write object to file (used for testing certain key-sequences)
     *
     * @param obj Linked list to write
     * @param filename filename to dump the object
     * @throws IOException is thrown when an I/O operation fails
     */
    @SuppressWarnings("unused")
    public static void writeObjectToFile(LinkedList<Long> obj,
                                         String filename) throws IOException {
        System.out.println("Writing object to: " + filename);
        FileOutputStream fout = new FileOutputStream(filename);
        ObjectOutputStream foutStream = new ObjectOutputStream(fout);
        foutStream.writeObject(obj);
        foutStream.close();
        System.out.println("Writing complete to file: " + filename);
    }

    /**
     * Load linked list object from file (used for testing certain key-sequences)
     *
     * @param filename file to load the object from
     * @return the object itself.
     * @throws IOException is thrown when an I/O operation fails
     * @throws ClassNotFoundException is thrown when the reflection is not able to find the correct class.
     */
    private static LinkedList<Long> loadListFromFile(String filename)
            throws IOException, ClassNotFoundException {
        System.out.println("Loading LinkedList<Long> object from file: " + filename);
        FileInputStream fin = new FileInputStream(filename);
        ObjectInputStream finStream = new ObjectInputStream(fin);
        @SuppressWarnings("unchecked")
        LinkedList<Long> l = (LinkedList<Long>)finStream.readObject();
        finStream.close();
        return l;
    }


    /**
     * This is a pseudo random number generator for unique
     * discreet values using quadratic prime residues.
     *
     * Taken from @preshing
     *
     */
    @SuppressWarnings("unused")
    public static class randQPR {
        static final long prime = 4294967291L;
        private final long inter_index;
        private long index;

        public randQPR(long seed, long seedOffset) {
            index = permQPR(permQPR(seed) + 0x682f0161L);
            inter_index = permQPR(permQPR(seedOffset) + 0x46790905L);
        }

        long permQPR(long x) {
            if (x >= prime) {
                return x;
            }
            long residue = (x * x) % prime;
            return (x <= prime / 2 ? residue : prime - residue);
        }

        public long next() {
            return (permQPR(permQPR(index++) + inter_index) ^ 0x5bf03635L);
        }
    }
}
