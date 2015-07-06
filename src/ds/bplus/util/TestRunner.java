package ds.bplus.util;

import ds.bplus.bptree.BPlusTreePerformanceCounter;

import java.io.IOException;

/**
 *
 * Another wrapper class that makes running tests a bit easier.
 *
 */

public class TestRunner {

    /**
     * Run the test interface
     *
     * @param bPerf performance class tied to a B+ Tree instance
     * @throws IOException
     */
    public static void runBench(BPlusTreePerformanceCounter bPerf)
            throws IOException, InvalidBTreeStateException {
        StandardInputRead sin = new StandardInputRead();
        int choice;

        while((choice = menuChoice(sin)) != 6)
            {handleChoices(choice, sin, bPerf);}
    }

    /**
     * Display menu choices and grab the user selection
     *
     * @param sin input class
     * @return a valid user option selection
     */
    public static int menuChoice(StandardInputRead sin) {
        System.out.println("\nSelect from menu\n");
        System.out.println("\t1) Run default trials");
        System.out.println("\t2) Run insertion run");
        System.out.println("\t3) Run deletion run");
        System.out.println("\t4) Run search run");
        System.out.println("\t5) Run range query run");
        System.out.println("\t6) Exit\n");
        int choice = sin.readPositiveInt("Enter your choice: ");
        while(!(choice > 0 && choice < 7))
            {choice = sin.readPositiveInt("Wrong range, try again: ");}
        return(choice);
    }

    /**
     * Silently just run the default trials using the default values
     *
     * @param bPerf performance class tied to a B+ Tree instance
     * @throws IOException
     */
    public static void runDefaultTrialsFast(BPlusTreePerformanceCounter bPerf)
            throws IOException, InvalidBTreeStateException {
        int trials = 20;
        int vmin = 1;
        int vmax = 99999;
        boolean verbose = false;
        boolean unique = false;
        String val = "asdfasdfas";
        int qrange = 150;
        runDefaultTrials(trials, vmin, vmax, qrange, val,
                unique, verbose, bPerf);
    }

    /**
     * Handle the selected user option
     *
     * @param choice the user choice
     * @param sin the input class
     * @param bPerf performance class ties to a B+ Tree instance
     * @throws IOException
     */
    public static void handleChoices(int choice, StandardInputRead sin,
                                     BPlusTreePerformanceCounter bPerf)
            throws IOException, InvalidBTreeStateException {
        boolean unique = true;
        switch(choice) {
            case 1: {
                int trials = 20;
                int vmin = 1;
                int vmax = 99999;
                boolean verbose = false;
                String val = "asdfasdfas";
                int qrange = 150;
                runDefaultTrials(trials, vmin, vmax, qrange, val,
                        false, verbose, bPerf);
                break;
            }
            case 2: {
                runInsertion(sin, bPerf);
                break;
            }
            case 3: {
                runDeletion(sin, bPerf);
                break;
            }
            case 4: {
                runSearch(sin, bPerf);
                break;
            }
            case 5: {
                runRangeQuery(sin, bPerf);
                break;
            }
            default: {
                System.out.println("Closing program.");
                break;
            }
        }
    }

    public static boolean isUnique(StandardInputRead sin) {
        System.out.println("Want unique results?");
        System.out.println("\t1) Yes");
        System.out.println("\t2) No");
        int choice = sin.readPositiveInt("Enter your choice: ");
        if(choice == 2) {
            return(false);
        } else if(choice == 1) {
            return(true);
        } else {
            System.out.println("Wrong choice, using default (Yes)");
            return(true);
        }
    }

    /**
     * Run insertion
     *
     * @param sin input class
     * @param bPerf performance class tied to a B+ Tree instance
     * @throws IOException
     */
    public static void runInsertion(StandardInputRead sin,
                                    BPlusTreePerformanceCounter bPerf)
            throws IOException, InvalidBTreeStateException {
        boolean unique = isUnique(sin);
        String val = "1234567890";  // default value
        int key;
        // get a key to insert
        while((key = sin.readPositiveInt("Enter a valid key: ")) == -1)
            {System.out.println("Wrong key... try again");}
        // all are verbose
        bPerf.insertIO(key, val, unique, true);
    }


    /**
     * Run deletion
     *
     * @param sin input class
     * @param bPerf performance class tied to a B+ Tree instance
     * @throws IOException
     */
    public static void runDeletion(StandardInputRead sin,
                                   BPlusTreePerformanceCounter bPerf)
            throws IOException {
        boolean unique = isUnique(sin);
        String val = "1234567890";  // default value
        int key = -1;
        // get a key to insert
        while((key = sin.readPositiveInt("Enter a valid key: ")) == -1)
            {System.out.println("Wrong key... try again");}
        // all are verbose
        //bPerf.insertIO(key, val, unique, true);
    }


    /**
     * Run a search instance
     *
     * @param sin input class
     * @param bPerf performance class tied to a B+ Tree instance
     * @throws IOException
     */
    public static void runSearch(StandardInputRead sin,
                                 BPlusTreePerformanceCounter bPerf)
            throws IOException {
        boolean unique = isUnique(sin);
        String val = "1234567890";  // default value
        int key = -1;
        // get a key to insert
        while((key = sin.readPositiveInt("Enter a valid key: ")) == -1)
            {System.out.println("Wrong key... try again");}
        // all are verbose
        bPerf.searchIO(key, unique, true);
    }

    /**
     * Run a range query instance
     *
     * @param sin input class
     * @param bPerf performance class tied to a B+ Tree instance
     * @throws IOException
     */
    public static void runRangeQuery(StandardInputRead sin,
                                     BPlusTreePerformanceCounter bPerf)
            throws IOException {
        boolean unique = isUnique(sin);
        String val = "1234567890";  // default value
        int minKey;
        int maxKey;
        // get a key to insert
        while((minKey = sin.readPositiveInt("Enter a valid min key: ")) == -1)
            {System.out.println("Wrong key... try again");}
        while((maxKey = sin.readPositiveInt("Enter a valid max key: ")) == -1)
            {System.out.println("Wrong key... try again");}

        if(maxKey < minKey)
            {System.out.println("Can't proceed maxKey < minKey"); return;}

        // all are verbose
        bPerf.rangeIO(minKey, maxKey, unique, true);
    }

    /**
     * Run default trial set
     *
     * @param trials number of trials to run
     * @param vmin min key value
     * @param vmax max key value
     * @param qrange range of range queries
     * @param val value of tied to the key (the same is used)
     * @param unique allow duplicates?
     * @param verbose verbose results?
     * @param bPerf performance class tied to a B+ Tree instance
     * @throws IOException
     */
    public static void runDefaultTrials(int trials, int vmin, int vmax, int qrange,
                                        String val, boolean unique, boolean verbose,
                                        BPlusTreePerformanceCounter bPerf)
            throws IOException, InvalidBTreeStateException {
        TrialsClass.runSearchTrial(trials, vmin, vmax, unique, bPerf, verbose);
        TrialsClass.runInsertTrial(trials, vmin, vmax, val, unique, bPerf, verbose);
        TrialsClass.runRangeQueryTrial(trials, vmin, vmax, qrange, unique, bPerf, verbose);
    }
}
