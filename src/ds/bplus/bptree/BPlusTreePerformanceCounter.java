package ds.bplus.bptree;

import ds.bplus.util.InvalidBTreeStateException;

import java.io.IOException;

public class BPlusTreePerformanceCounter {
    private int totalNodeReads;
    private int totalInternalNodeReads;
    private int totalLeafNodeReads;
    private int totalOverflowReads;

    private int totalNodeWrites;
    private int totalInternalNodeWrites;
    private int totalLeafNodeWrites;
    private int totalOverflowWrites;


    private int totalInsertionReads;
    private int totalDeletionReads;
    private int totalSearchReads;
    private int totalRangeQueryReads;
    private int totalInsertionWrites;
    private int totalDeletionWrites;
    private int totalSearchWrites;
    private int totalRangeQueryWrites;

    private int pageReads;
    private int pageWrites;

    private int pageInternalReads;
    private int pageLeafReads;
    private int pageOverflowReads;

    private int pageInternalWrites;
    private int pageLeafWrites;
    private int pageOverflowWrites;

    private int totalInsertions;
    private int totalDeletions;
    private int totalSearches;
    private int totalRangeQueries;

    private int totalSplits;
    private int totalRootSplits;
    private int totalInternalNodeSplits;
    private int totalLeafSplits;


    private int totalPages;
    private int totalOverflowPages;
    private int totalInternalNodes;
    private int totalLeaves;

    private int totalInternalNodeDeletions;
    private int totalLeafNodeDeletions;
    private int totalOverflowPagesDeletions;

    private boolean trackIO;
    private BPlusTree bt = null;

    public BPlusTreePerformanceCounter(boolean trackIO) {
        this.trackIO = trackIO;
        resetAllMetrics();
    }

    public void setTrackIO(boolean trackIO) {
        this.trackIO = trackIO;
    }

    public void setBTree(BPlusTree bt) {
        this.bt = bt;
    }

    public void incrementTotalPages() {
        if(trackIO) {
            totalPages++;
        }
    }

    public void incrementTotalOverflowPages() {
        if(trackIO) {
            totalOverflowPages++;
            incrementTotalPages();
        }
    }

    public void incrementTotalInternalNodes() {
        if(trackIO) {
            totalInternalNodes++;
            incrementTotalPages();
        }
    }

    public void incrementTotalLeaves() {
        if(trackIO) {
            totalLeaves++;
            incrementTotalPages();
        }
    }

    public void incrementTotalNodeReads() {
        if(trackIO) {
            totalNodeReads++;
        }
    }

    public void incrementTotalNodeWrites() {
        if(trackIO) {
            totalNodeWrites++;
        }
    }

    public void incrementTotalInsertions() {
        if(trackIO) {
            totalInsertions++;
        }
    }

    public void incrementTotalDeletions() {
        if(trackIO) {
            totalDeletions++;
        }
    }

    public void incrementTotalInternalNodeDeletions() {
        if(trackIO) {
            totalInternalNodeDeletions++;
            incrementTotalDeletions();
        }
    }

    public void incrementTotalLeafNodeDeletions() {
        if(trackIO) {
            totalLeafNodeDeletions++;
            incrementTotalDeletions();
        }
    }

    public void incrementTotalOverflowPageDeletions() {
        if(trackIO) {
            totalOverflowPagesDeletions++;
            incrementTotalDeletions();
        }
    }

    public void incrementTotalSearches() {
        if(trackIO) {
            totalSearches++;
        }
    }

    public void incrementTotalRangeQueries() {
        if(trackIO) {
            totalRangeQueries++;
        }
    }

    public void incrementTotalSplits() {
        if(trackIO) {
            totalSplits++;
        }
    }

    public void incrementRootSplits() {
        if(trackIO) {
            totalRootSplits++;
            incrementTotalSplits();
        }
    }

    public void incrementInternalNodeSplits() {
        if(trackIO) {
            totalInternalNodeSplits++;
            incrementTotalSplits();
        }
    }

    public void incrementTotalLeafSplits() {
        if(trackIO) {
            totalLeafSplits++;
            incrementTotalSplits();
        }
    }

    public void incrementPageReads() {
        if(trackIO) {
            pageReads++;
        }
    }

    public void incrementPageWrites() {
        if(trackIO) {
            pageWrites++;
        }
    }

    public void startPageTracking() {
        pageReads = 0;
        pageWrites = 0;
        pageInternalReads = 0;
        pageLeafReads = 0;
        pageOverflowReads = 0;

        pageInternalWrites = 0;
        pageLeafWrites = 0;
        pageOverflowWrites = 0;
    }

    private void resetIntermittentPageTracking() {
        pageReads = 0;
        pageWrites = 0;
        pageInternalReads = 0;
        pageLeafReads = 0;
        pageOverflowReads = 0;
        pageInternalWrites = 0;
        pageLeafWrites = 0;
        pageOverflowWrites = 0;
    }

    public int getPageReads() {
        return(pageReads);
    }

    public int getPageWrites() {
        return(pageWrites);
    }

    public int getInterminentInternalPageReads() {
        return(pageInternalReads);
    }

    public int getInterminentLeafPageReads() {
        return(pageLeafReads);
    }

    public int getInterminentOverflowPageReads() {
        return(pageOverflowReads);
    }

    public int getInterminentInternalPageWrites() {
        return(pageInternalWrites);
    }

    public int getInterminentLeafPageWrites() {
        return(pageLeafWrites);
    }

    public int getInterminentOverflowPageWrites() {
        return(pageOverflowWrites);
    }

    public int[] deleteIO(long key, boolean unique, boolean verbose)
            throws IOException {
        startPageTracking();
        bt.searchKey(key, unique);
        if(verbose) {
            System.out.println("Total page reads for this deletion: " + getPageReads());
            System.out.println("Total page writes for this deletion: " + getPageWrites());
            System.out.println("\nBroken down statistics: ");
            System.out.println("\tInternal node (reads, writes): " +
                    getInterminentInternalPageReads() + ", " +
                    getInterminentInternalPageWrites());
            System.out.println("\tLeaf node (reads, writes): " +
                    getInterminentLeafPageReads() + ", " +
                    getInterminentInternalPageWrites());
            System.out.println("\tOverflow node (reads, writes): " +
                    getInterminentOverflowPageReads() + ", " +
                    getInterminentOverflowPageWrites());
        }
        int res[] = new int[8];
        res[0] = getPageReads();
        res[1] = getPageWrites();
        res[2] = getInterminentInternalPageReads();
        res[3] = getInterminentInternalPageWrites();
        res[4] = getInterminentLeafPageReads();
        res[5] = getInterminentLeafPageWrites();
        res[6] = getInterminentOverflowPageReads();
        res[7] = getInterminentOverflowPageWrites();
        totalDeletionReads += pageReads;
        totalDeletionWrites += pageWrites;

        resetIntermittentPageTracking();
        return res;
    }

    public int[] searchIO(long key, boolean unique, boolean verbose)
            throws IOException {
        startPageTracking();
        SearchResult r = bt.searchKey(key, unique);
        if(verbose) {
            System.out.println("Total page reads for this search: " + getPageReads());
            System.out.println("Total page writes for this search: " + getPageWrites());
            System.out.println("\nBroken down statistics: ");
            System.out.println("\tInternal node (reads, writes): " +
                    getInterminentInternalPageReads() + ", " +
                    getInterminentInternalPageWrites());
            System.out.println("\tLeaf node (reads, writes): " +
                    getInterminentLeafPageReads() + ", " +
                    getInterminentInternalPageWrites());
            System.out.println("\tOverflow node (reads, writes): " +
                    getInterminentOverflowPageReads() + ", " +
                    getInterminentOverflowPageWrites());
        }
        int res[] = new int[9];
        res[0] = getPageReads();
        res[1] = getPageWrites();
        res[2] = getInterminentInternalPageReads();
        res[3] = getInterminentInternalPageWrites();
        res[4] = getInterminentLeafPageReads();
        res[5] = getInterminentLeafPageWrites();
        res[6] = getInterminentOverflowPageReads();
        res[7] = getInterminentOverflowPageWrites();
        res[8] = r.isFound() ? 1 : 0;
        totalSearchReads += pageReads;
        totalSearchWrites += pageWrites;

        resetIntermittentPageTracking();
        return res;
    }

    public int[] rangeIO(long minKey, long maxKey,
                       boolean unique, boolean verbose) throws IOException {
        startPageTracking();
        bt.rangeSearch(minKey, maxKey, unique);
        if(verbose) {
            System.out.println("Total page reads for this search: " + getPageReads());
            System.out.println("Total page writes for this search: " + getPageWrites());
            System.out.println("\nBroken down statistics: ");
            System.out.println("\tInternal node (reads, writes): " +
                    getInterminentInternalPageReads() + ", " +
                    getInterminentInternalPageWrites());
            System.out.println("\tLeaf node (reads, writes): " +
                    getInterminentLeafPageReads() + ", " +
                    getInterminentInternalPageWrites());
            System.out.println("\tOverflow node (reads, writes): " +
                    getInterminentOverflowPageReads() + ", " +
                    getInterminentOverflowPageWrites());
        }
        int res[] = new int[8];
        res[0] = getPageReads();
        res[1] = getPageWrites();
        res[2] = getInterminentInternalPageReads();
        res[3] = getInterminentInternalPageWrites();
        res[4] = getInterminentLeafPageReads();
        res[5] = getInterminentLeafPageWrites();
        res[6] = getInterminentOverflowPageReads();
        res[7] = getInterminentOverflowPageWrites();
        totalRangeQueryReads += pageReads;
        totalRangeQueryWrites += pageWrites;

        resetIntermittentPageTracking();
        return res;
    }

    public int[] insertIO(long key, String value,
                        boolean unique, boolean verbose)
            throws IOException, InvalidBTreeStateException {
        startPageTracking();
        bt.insertKey(key, value, unique);
        if(verbose) {
            System.out.println("Total page reads for this insertion: " + getPageReads());
            System.out.println("Total page writes for this insertion: " + getPageWrites());
            System.out.println("\nBroken down statistics: ");
            System.out.println("\tInternal node (reads, writes): " +
                    getInterminentInternalPageReads() + ", " +
                    getInterminentInternalPageWrites());
            System.out.println("\tLeaf node (reads, writes): " +
                    getInterminentLeafPageReads() + ", " +
                    getInterminentInternalPageWrites());
            System.out.println("\tOverflow node (reads, writes): " +
                    getInterminentOverflowPageReads() + ", " +
                    getInterminentOverflowPageWrites());
        }
        int res[] = new int[8];
        res[0] = getPageReads();
        res[1] = getPageWrites();
        res[2] = getInterminentInternalPageReads();
        res[3] = getInterminentInternalPageWrites();
        res[4] = getInterminentLeafPageReads();
        res[5] = getInterminentLeafPageWrites();
        res[6] = getInterminentOverflowPageReads();
        res[7] = getInterminentOverflowPageWrites();

        totalInsertionReads += pageReads;
        totalInsertionWrites += pageReads;

        resetIntermittentPageTracking();
        return res;
    }

    public int getTotalIntermittentInsertionReads() {
        return(totalInsertionReads);
    }

    public int getTotalIntermittentInsertionWrites() {
        return(totalInsertionWrites);
    }

    public void incrementIntermittentInternalNodeReads() {
        if(trackIO) {
            pageInternalReads++;
            incrementPageReads();
        }
    }

    public void incrementIntermittentLeafNodeReads() {
        if(trackIO) {
            pageLeafReads++;
            incrementPageReads();
        }
    }

    public void incrementIntermittentOverflowPageReads() {
        if(trackIO) {
            pageOverflowReads++;
            incrementPageReads();
        }
    }

    public void incrementIntermittentInternalNodeWrites() {
        if(trackIO) {
            pageInternalWrites++;
            incrementPageWrites();
        }
    }

    public void incrementIntermittentLeafNodeWrites() {
        if(trackIO) {
            pageLeafWrites++;
            incrementPageWrites();
        }
    }

    public void incrementIntermittentOverflowPageWrites() {
        if(trackIO) {
            pageOverflowWrites++;
            incrementPageWrites();
        }
    }


    public void incrementTotalInternalNodeReads() {
        if(trackIO) {
            totalInternalNodeReads++;
            incrementTotalNodeReads();
            incrementIntermittentInternalNodeReads();
        }
    }

    public void incrementTotalLeafNodeReads() {
        if(trackIO) {
            totalLeafNodeReads++;
            incrementTotalNodeReads();
            incrementIntermittentLeafNodeReads();
        }
    }

    public void incrementTotalOverflowReads() {
        if(trackIO) {
            totalOverflowReads++;
            incrementTotalNodeReads();
            incrementIntermittentOverflowPageReads();
        }
    }

    public void incrementTotalInternalNodeWrites() {
        if(trackIO) {
            totalInternalNodeWrites++;
            incrementTotalNodeWrites();
            incrementIntermittentInternalNodeWrites();
        }
    }

    public void incrementTotalLeafNodeWrites() {
        if(trackIO) {
            totalLeafNodeWrites++;
            incrementTotalNodeWrites();
            incrementIntermittentLeafNodeWrites();
        }
    }

    public void incrementTotalOverflowNodeWrites() {
        if(trackIO) {
            totalOverflowWrites++;
            incrementTotalNodeWrites();
            incrementIntermittentOverflowPageWrites();
        }
    }

    public int totalOperationCount() {
        return(totalInsertions + totalSearches +
                totalRangeQueries + totalDeletions);
    }

    public void printTotalStatistics() {
        System.out.println("\n !! Printing total recorded statistics !!");
        System.out.println("\nOperations break down");
        System.out.println("\n\tTotal insertions: " + totalInsertions);
        System.out.println("\tTotal searches: " + totalSearches);
        System.out.println("\tTotal range queries: " + totalRangeQueries);
        System.out.println("\tTotal performed op count: " + totalOperationCount());

        System.out.println("\nTotal I/O break down (this run only)");
        System.out.println("\nTotal Read statistics");
        System.out.println("\n\tTotal reads: " + totalNodeReads);
        System.out.println("\tTotal Internal node reads: " + totalInternalNodeReads);
        System.out.println("\tTotal Leaf node reads: " + totalLeafNodeReads);
        System.out.println("\tTotal Overflow node reads: " + totalOverflowReads);

        System.out.println("\nTotal Write statistics: ");
        System.out.println("\n\tTotal writes: " + totalNodeWrites);
        System.out.println("\tTotal Internal node writes: " + totalInternalNodeWrites);
        System.out.println("\tTotal Leaf node writes: " + totalLeafNodeWrites);
        System.out.println("\tTotal Overflow node writes: " + totalOverflowWrites);

        System.out.println("\nPage creation break down.");
        System.out.println("\n\tTotal pages created: " + totalPages);
        System.out.println("\tTotal Internal nodes created: " + totalInternalNodes);
        System.out.println("\tTotal Leaf nodes created: " + totalLeaves);
        System.out.println("\tTotal Overflow nodes created: " + totalOverflowPages);

        System.out.println("\nPage deletion break down.");
        System.out.println("\n\tTotal pages deleted: " + totalDeletions);
        System.out.println("\tTotal Internal nodes deleted: " + totalInternalNodeDeletions);
        System.out.println("\tTotal Leaf nodes deleted: " + totalLeafNodeDeletions);
        System.out.println("\tTotal Overflow pages deleted: " + totalOverflowPagesDeletions);

        System.out.println("\nPage split statistics");
        System.out.println("\n\tTotal page splits: " + totalSplits);
        System.out.println("\tActual Root splits: " + totalRootSplits);
        System.out.println("\tInternal node splits: " + totalInternalNodeSplits);
        System.out.println("\tLeaf node splits: " + totalLeafSplits);
    }

    public void resetAllMetrics() {
        totalPages = 0;
        totalInternalNodes = 0;
        totalLeaves = 0;
        totalOverflowPages = 0;

        totalNodeReads = 0;
        totalInternalNodeReads = 0;
        totalOverflowReads = 0;
        totalLeafNodeReads = 0;

        totalNodeWrites = 0;
        totalInternalNodeWrites = 0;
        totalLeafNodeWrites = 0;
        totalOverflowWrites = 0;

        totalInternalNodeDeletions = 0;
        totalLeafNodeDeletions = 0;
        totalOverflowPagesDeletions = 0;

        totalDeletions = 0;
        totalInsertions = 0;
        totalSearches = 0;
        totalRangeQueries = 0;

        totalSplits = 0;
        totalRootSplits = 0;
        totalInternalNodeSplits = 0;
        totalLeafSplits = 0;

        pageReads = 0;
        pageWrites = 0;
        pageInternalReads = 0;
        pageLeafReads = 0;
        pageOverflowReads = 0;

        pageInternalWrites = 0;
        pageLeafWrites = 0;
        pageOverflowWrites = 0;

        totalSearchReads = 0;
        totalSearchWrites = 0;
        totalRangeQueryReads = 0;
        totalRangeQueryWrites = 0;
        totalInsertionReads = 0;
        totalInsertionWrites = 0;
        totalDeletionReads = 0;
        totalDeletionWrites = 0;
    }
}
