package tuc.ds.bptree;

public class BPlusTreePerformanceCounter {
    private int totalNodeReads;
    private int totalNodeWrites;
    private int totalInsertions;
    private int totalSearches;
    private int totalRangeQueries;
    private int totalSplits;
    private int totalRootSplits;
    private int totalInternalNodeSplits;
    private int totalLeafSplits;
    private int totalOverflowSplits;

    // average read metrics
    private double averageReadsPerSearch;
    private double averageReadsPerRangeSearch;

    // average write metrics
    private double averageWritesPerInsertion;

    private int totalPages;
    private int totalOverflowPages;


    private void incrementTotalNodeReads()
        {totalNodeReads++;}

    public void incrementTotalNodeWrites()
        {totalNodeWrites++;}

    public void incrementTotalInsertions()
        {totalInsertions++;}

    public void incrementTotalSearches()
        {totalSearches++;}

    public void incrementTotalRangeQueries()
        {totalRangeQueries++;}

    public void incrementTotalSplits()
        {totalSplits++;}

    public void incrementRootSplits()
        {totalRootSplits++;}

    public void incrementInternalNodeSplits()
        {totalInternalNodeSplits++;}

    public void incrementTotalLeafSplits()
        {totalLeafSplits++;}

    public void incremeentTotalOverflowSplits()
        {totalOverflowSplits++;}

    public BPlusTreePerformanceCounter() {
        resetAllMetrics();
    }

    public void resetAllMetrics() {
        totalNodeReads = 0;
        totalNodeWrites = 0;
        totalInsertions = 0;
        totalSearches = 0;
        totalRangeQueries = 0;
        totalSplits = 0;
        totalRootSplits = 0;
        totalInternalNodeSplits = 0;
        totalLeafSplits = 0;
        totalOverflowSplits = 0;
    }
}
