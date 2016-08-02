package ds.bplus.util;

import ds.bplus.bptree.BPlusTreePerformanceCounter;

import java.io.IOException;

/**
 *
 * Wrapper class to run trials for a specific functionality
 *
 */
@SuppressWarnings("unused")
class TrialsClass {

    /**
     * Run a search trial
     *
     * @param trials the number of trials to run
     * @param rmin the min key value
     * @param rmax the max key value
     * @param unique want unique results?
     * @param bPerf performance class tied to a B+ Tree instance
     * @param verbose verbose results?
     * @throws IOException is thrown when an I/O operation fails
     * @throws InvalidBTreeStateException is thrown when there are inconsistencies in the blocks.
     */
    static void runSearchTrial(int trials, int rmin, int rmax, boolean unique,
                               BPlusTreePerformanceCounter bPerf,
                               boolean verbose)
            throws IOException, InvalidBTreeStateException {

        int pageReads = 0;
        int pageWrites = 0;
        //int found = 0;
        int stats[];

        // trial loop
        for(int i = 0; i < trials; i++) {
            stats = bPerf.searchIO(Utilities.randInt(rmin, rmax), unique, verbose);
            pageReads += stats[0];
            pageWrites += stats[1];
        }

        System.out.println("\nPerformed " + trials + " search trials");
        System.out.println("\n\tTotal page reads: " + pageReads);
        System.out.println("\tTotal page writes: " + pageWrites);
        System.out.println("\tAverage (reads, writes): " + (pageReads/(1.0*trials))
                + ", " + (pageWrites/(1.0*trials)));
    }

    /**
     * Run a insertion trial
     *
     * @param trials the number of trials to run
     * @param rmin the min key value
     * @param rmax the max key value
     * @param value value to tie with the inserted key
     * @param unique allow duplicate insertions?
     * @param bPerf performance class tied to a B+ Tree instance
     * @param verbose verbose results?
     * @throws IOException is thrown when an I/O operation fails
     * @throws InvalidBTreeStateException is thrown when there are inconsistencies in the blocks.
     */
    static void runInsertTrial(int trials, int rmin, int rmax,
                               String value, boolean unique,
                               BPlusTreePerformanceCounter bPerf, boolean verbose)
            throws IOException, InvalidBTreeStateException {
        int pageReads = 0;
        int pageWrites = 0;
        Long key;
        int stats[];

        // trial loop
        for(int i = 0; i < trials; i++) {
            key = (long) Utilities.randInt(rmin, rmax);
            stats = bPerf.insertIO(key,
                    value == null ? key.toString() : value, unique, verbose);
            pageReads += stats[0];
            pageWrites += stats[1];
        }

        System.out.println("\nPerformed " + trials + " insertion trials");
        System.out.println("\n\tTotal page reads: " + pageReads);
        System.out.println("\tTotal page writes: " + pageWrites);
        System.out.println("\tAverage (reads, writes): " + (pageReads/(1.0*trials))
                + ", " + (pageWrites/(1.0*trials)));
    }

    /**
     * Run a range query trial
     *
     * @param trials the number of trials to run
     * @param rmin the min key value
     * @param rmax the max key value
     * @param range value to tie with the inserted key
     * @param unique allow duplicate insertions?
     * @param bPerf performance class tied to a B+ Tree instance
     * @param verbose verbose results?
     * @throws IOException is thrown when an I/O operation fails
     * @throws InvalidBTreeStateException is thrown when there are inconsistencies in the blocks.
     */
    static void runRangeQueryTrial(int trials, int rmin, int rmax, int range,
                                   boolean unique, BPlusTreePerformanceCounter bPerf,
                                   boolean verbose) throws IOException, InvalidBTreeStateException {
        int pageReads = 0;
        int pageWrites = 0;
        int stats[];
        int rtmax = rmax - range;
        int arange = Utilities.randInt(rmin, rtmax);

        // trial loop
        for(int i = 0; i < trials; i++) {
            stats = bPerf.rangeIO(arange, arange+range, unique, verbose);
            pageReads += stats[0];
            pageWrites += stats[1];
        }

        System.out.println("\nPerformed " + trials + " Range Query trials");
        System.out.println("\n\tTotal page reads: " + pageReads);
        System.out.println("\tTotal page writes: " + pageWrites);
        System.out.println("\tAverage (reads, writes): " + (pageReads/(1.0*trials))
                + ", " + (pageWrites/(1.0*trials)));
    }

    /**
     * Run a deletion trial
     *
     * @param trials number of trials to run
     * @param rmin the min key value
     * @param rmax the max key value
     * @param unique delete the *first* found or *all* found?
     * @param bPerf performance class tied to a B+ Tree instance
     * @param verbose verbose results?
     * @throws IOException is thrown when an I/O operation fails
     * @throws InvalidBTreeStateException is thrown when there are inconsistencies in the blocks.
     */
    static void runDeletionTrials(int trials, int rmin, int rmax, boolean unique,
                                  BPlusTreePerformanceCounter bPerf,
                                  boolean verbose)
            throws IOException, InvalidBTreeStateException {
        int pageReads = 0;
        int pageWrites = 0;
        int stats[];

        // trial loop
        for(int i = 0; i < trials; i++) {
            stats = bPerf.deleteIO(Utilities.randInt(rmin, rmax), unique, verbose);
            pageReads += stats[0];
            pageWrites += stats[1];
        }

        System.out.println("\nPerformed " + trials + " deletion trials");
        System.out.println("\n\tTotal page reads: " + pageReads);
        System.out.println("\tTotal page writes: " + pageWrites);
        System.out.println("\tAverage (reads, writes): " + (pageReads/(1.0*trials))
                + ", " + (pageWrites/(1.0*trials)));
    }
}
