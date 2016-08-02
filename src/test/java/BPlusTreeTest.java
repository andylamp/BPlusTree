import ds.bplus.bptree.BPlusConfiguration;
import ds.bplus.bptree.BPlusTree;
import ds.bplus.bptree.BPlusTreePerformanceCounter;
import ds.bplus.util.Utilities;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;

/** 
* BPlusTree Tester. 
*
* @since <pre>Jul 28, 2015</pre> 
* @version 1.0 
*/ 
public class BPlusTreeTest {
    private String satelliteValue;
    private boolean uniqueEntries;
    private boolean verboseResults;
    private int startKey;
    private int endKey;
    private int totalKeys;
    private boolean recreateTree;

    private BPlusConfiguration btConf256;
    private BPlusConfiguration btConf1024;
    private BPlusConfiguration btConf2048;

    private BPlusTreePerformanceCounter bPerf256;
    private BPlusTreePerformanceCounter bPerf1024;
    private BPlusTreePerformanceCounter bPerf2048;

   private BPlusTree bt256;
   private BPlusTree bt1024;
   private BPlusTree bt2048;

   @Before
   public void before() throws Exception {
      System.out.println("Before test");
       startKey = 0;
       endKey = 10000;
       totalKeys = endKey - startKey;
       satelliteValue = " ";
   }

   @After
   public void after() throws Exception {
       //System.out.println("After test");
       bt256.commitTree();
       bt1024.commitTree();
       bt2048.commitTree();
   }

   /**
    *
    * This test loads up sequentially a massive key list
    * (10^5) onto trees of the following degrees:
    *
    *    - Page sizes: 256, 1024 (1Kb), 2048 (2Kb)
    *
    * with the following (Key, Value) settings:
    *
    *    - Satellite data size: 20 Bytes each entry
    *    - Key size: 8 bytes
    *
    * @throws Exception is thrown when an error is catch'ed in any of the operations performed.
    */
   @Test
   public void testMassSequentialInsertions() throws Exception {
      uniqueEntries = true;
      verboseResults = false;
      recreateTree = true;

      // initialize the configuration
      btConf256 = new BPlusConfiguration(256);
      btConf1024 = new BPlusConfiguration(1024);
      btConf2048 = new BPlusConfiguration(2048);

      // set up the the counters for each tree
      bPerf256 = new BPlusTreePerformanceCounter(true);
      bPerf1024 = new BPlusTreePerformanceCounter(true);
      bPerf2048 = new BPlusTreePerformanceCounter(true);

      // finally setup the tree instances
      bt256 = new BPlusTree(btConf256, recreateTree ? "rw+" : "rw",
              "tree256.bin", bPerf256);
      bt1024 = new BPlusTree(btConf1024, recreateTree ? "rw+" : "rw",
              "tree1024.bin", bPerf1024);
      bt2048 = new BPlusTree(btConf2048, recreateTree ? "rw+" : "rw",
              "tree2048.bin", bPerf2048);

      // now set up the insertions
      Utilities.sequentialAddToTree(startKey, endKey,
              satelliteValue, uniqueEntries, bt256);

      Utilities.sequentialAddToTree(startKey, endKey,
              satelliteValue, uniqueEntries, bt1024);

      Utilities.sequentialAddToTree(startKey, endKey,
              satelliteValue, uniqueEntries, bt2048);

      // now search
      int found_cnt256 = 0;
      int found_cnt1024 = 0;
      int found_cnt2048 = 0;

      int[] res256, res1024, res2048;
      for(int i = startKey; i < endKey; i++) {
         res256 = bPerf256.searchIO(i, uniqueEntries, verboseResults);
         res1024 = bPerf1024.searchIO(i, uniqueEntries, verboseResults);
         res2048 = bPerf2048.searchIO(i, uniqueEntries, verboseResults);

         if(res256[8] == 1) {found_cnt256++;}
         if(res1024[8] == 1) {found_cnt1024++;}
         if(res2048[8] == 1) {found_cnt2048++;}
      }

      // check result numbers
      if(found_cnt256 != totalKeys)
         {throw new Exception("BTree with page size: 256 failed to find all keys");}

      if(found_cnt1024 != totalKeys)
         {throw new Exception("BTree with page size: 1024 failed to find all keys");}

      if(found_cnt2048 != totalKeys)
         {throw new Exception("BTree with page size: 2048 failed to find all keys");}
   }

    /**
     *
     * This test loads up sequentially a massive key list
     * (10^5) onto trees of the following degrees:
     *
     *    - Page sizes: 256, 1024 (1Kb), 2048 (2Kb)
     *
     * with the following (Key, Value) settings:
     *
     *    - Satellite data size: 20 Bytes each entry
     *    - Key size: 8 bytes
     *
     * In the end they are deleted as well.
     * @throws Exception is thrown when an error is catch'ed in any of the operations performed.
     */
    @Test
    public void testMassSequentialInsertionsWithDelete() throws Exception {
      uniqueEntries = true;
      verboseResults = false;
      recreateTree = true;

      // initialize the configuration
      btConf256 = new BPlusConfiguration(256);
      btConf1024 = new BPlusConfiguration(1024);
      btConf2048 = new BPlusConfiguration(2048);

      // set up the the counters for each tree
      bPerf256 = new BPlusTreePerformanceCounter(true);
      bPerf1024 = new BPlusTreePerformanceCounter(true);
      bPerf2048 = new BPlusTreePerformanceCounter(true);

      // finally setup the tree instances
      bt256 = new BPlusTree(btConf256, recreateTree ? "rw+" : "rw",
              "tree256.bin", bPerf256);
      bt1024 = new BPlusTree(btConf1024, recreateTree ? "rw+" : "rw",
              "tree1024.bin", bPerf1024);
      bt2048 = new BPlusTree(btConf2048, recreateTree ? "rw+" : "rw",
              "tree2048.bin", bPerf2048);

      // now set up the insertions
      Utilities.sequentialAddToTree(startKey, endKey,
              satelliteValue, uniqueEntries, bt256);

      Utilities.sequentialAddToTree(startKey, endKey,
              satelliteValue, uniqueEntries, bt1024);

      Utilities.sequentialAddToTree(startKey, endKey,
              satelliteValue, uniqueEntries, bt2048);

      // now search
      int found_cnt256 = 0;
      int found_cnt1024 = 0;
      int found_cnt2048 = 0;

      int[] res256, res1024, res2048;
      for(int i = startKey; i < endKey; i++) {
          res256 = bPerf256.deleteIO(i, uniqueEntries, verboseResults);
          res1024 = bPerf1024.deleteIO(i, uniqueEntries, verboseResults);
          res2048 = bPerf2048.deleteIO(i, uniqueEntries, verboseResults);

          //bt256.commitLookupPage();
          //bt1024.commitLookupPage();
          //bt2048.commitLookupPage();
          if (res256[8] == 1) {
              found_cnt256++;
          }
          if (res1024[8] == 1) {
              found_cnt1024++;
          }
          if (res2048[8] == 1) {
              found_cnt2048++;
          }
      }

      // check result numbers
      if(found_cnt256 != totalKeys)
         {throw new Exception("BTree with page size: 256 failed to find all keys");}

      if(found_cnt1024 != totalKeys)
         {throw new Exception("BTree with page size: 1024 failed to find all keys");}

      if(found_cnt2048 != totalKeys)
         {throw new Exception("BTree with page size: 2048 failed to find all keys");}
    }

   /**
    * This test loads up a massive unique key list in
    * random order (10^5) onto trees of the following degrees:
    *
    *    - Page sizes: 256, 1024 (1Kb), 2048 (2Kb)
    *
    * with the following (Key, Value) settings:
    *
    *    - Satellite data size: 20
    * @throws Exception is thrown when an error is catch'ed in any of the operations performed.
    */
   @Test
   public void testMassRandomUniqueInsertions() throws Exception {
       uniqueEntries = true;
       verboseResults = false;
       recreateTree = true;

       LinkedList<Long> bt256val, bt1024val, bt2048val;


       // initialize the configuration
       btConf256 = new BPlusConfiguration(256);
       btConf1024 = new BPlusConfiguration(1024);
       btConf2048 = new BPlusConfiguration(2048);

       // set up the the counters for each tree
       bPerf256 = new BPlusTreePerformanceCounter(true);
       bPerf1024 = new BPlusTreePerformanceCounter(true);
       bPerf2048 = new BPlusTreePerformanceCounter(true);

       // finally setup the tree instances
       bt256 = new BPlusTree(btConf256, recreateTree ? "rw+" : "rw",
               "tree256.bin", bPerf256);
       bt1024 = new BPlusTree(btConf1024, recreateTree ? "rw+" : "rw",
               "tree1024.bin", bPerf1024);
       bt2048 = new BPlusTree(btConf1024, recreateTree ? "rw+" : "rw",
               "tree2048.bin", bPerf2048);

       // randomly add non-unique insertions
       bt256val = Utilities.fuzzyAddToTree(startKey, endKey,
               uniqueEntries, bt256);

       bt1024val = Utilities.fuzzyAddToTree(startKey, endKey,
               uniqueEntries, bt1024);
       bt2048val = Utilities.fuzzyAddToTree(startKey, endKey,
               uniqueEntries, bt2048);

       // now search
       int found_cnt256 = 0;
       int found_cnt1024 = 0;
       int found_cnt2048 = 0;

       int[] res256, res1024, res2048;

       System.out.println("\n--> Dataset size: " + endKey + "\n");
       for(int i = startKey; i < endKey; i++) {
           res256 = bPerf256.searchIO(bt256val.pop(), uniqueEntries,
                   verboseResults);
           res1024 = bPerf1024.searchIO(bt1024val.pop(), uniqueEntries,
                   verboseResults);
           res2048 = bPerf2048.searchIO(bt2048val.pop(), uniqueEntries,
                   verboseResults);

           if(res256[8] == 1) {found_cnt256++;}
           if(res1024[8] == 1) {found_cnt1024++;}
           if(res2048[8] == 1) {found_cnt2048++;}
       }

       System.out.println("Total pages for bt256 in the end: " +
               bt256.getTotalTreePages());
       System.out.println("Total pages for bt1024 in the end: " +
               bt1024.getTotalTreePages());
       System.out.println("Total pages for bt2048 in the end: " +
               bt2048.getTotalTreePages());

       // check result numbers
       if(found_cnt256 != totalKeys)
          {throw new Exception("BTree with page size: 256 failed to find all keys");}

       if(found_cnt1024 != totalKeys)
          {throw new Exception("BTree with page size: 1024 failed to find all keys");}

       if(found_cnt2048 != totalKeys)
          {throw new Exception("BTree with page size: 2048 failed to find all keys");}
   }

   /**
    * This test loads up a massive non-unique key list in
    * random order (10^5) onto trees of the following degrees:
    *
    *    - Page sizes: 256, 1024 (1Kb), 2048 (2Kb)
    *
    * with the following (Key, Value) settings:
    *
    *    - Satellite data size: 20 Bytes each entry
    *    - Key size: 8 bytes
    *
    * After insertion each of the keys are searched.
    * @throws Exception is thrown when an error is catch'ed in any of the operations performed.
    */
   @Test
   public void testMassRandomInsertionsWithSearch() throws Exception {
      uniqueEntries = false;
      verboseResults = false;
       recreateTree = true;

      LinkedList<Long> bt256val, bt1024val, bt2048val;


      // initialize the configuration
      btConf256 = new BPlusConfiguration(256);
      btConf1024 = new BPlusConfiguration(1024);
      btConf2048 = new BPlusConfiguration(2048);

      // set up the the counters for each tree
      bPerf256 = new BPlusTreePerformanceCounter(true);
      bPerf1024 = new BPlusTreePerformanceCounter(true);
      bPerf2048 = new BPlusTreePerformanceCounter(true);

      // finally setup the tree instances
      bt256 = new BPlusTree(btConf256, recreateTree ? "rw+" : "rw",
              "tree256.bin", bPerf256);
      bt1024 = new BPlusTree(btConf1024, recreateTree ? "rw+" : "rw",
              "tree1024.bin", bPerf1024);
      bt2048 = new BPlusTree(btConf1024, recreateTree ? "rw+" : "rw",
              "tree2048.bin", bPerf2048);

      // randomly add non-unique insertions
      bt256val = Utilities.fuzzyAddToTree(startKey, endKey,
              uniqueEntries, bt256);


      //bt256val = Utilities.addToTreeFromList("list.ser", satelliteValue, uniqueEntries, bt256);

      //bt1024val = Utilities.addToTreeFromList("del.ser", satelliteValue, uniqueEntries, bt1024);
      //bt1024val = Utilities.addToTreeFromList("delsmall.ser", satelliteValue, uniqueEntries, bt1024);
      //bt1024val = Utilities.addToTreeFromList("lfile.ser", satelliteValue, uniqueEntries, bt1024);
      //bt1024val = Utilities.addToTreeFromList("lfileex.ser", satelliteValue, uniqueEntries, bt1024);

      bt1024val = Utilities.fuzzyAddToTree(startKey, endKey,
              uniqueEntries, bt1024);
      bt2048val = Utilities.fuzzyAddToTree(startKey, endKey,
              uniqueEntries, bt2048);

      // now search
      int found_cnt256 = 0;
      int found_cnt1024 = 0;
      int found_cnt2048 = 0;

      int[] res256, res1024, res2048;
      //Utilities.writeObjectToFile(bt1024val, "delsmall.ser");
      //bPerf1024.searchIO(2, false, false);
      //bPerf1024.searchIO(0, false, false);
      //endKey = bt1024val.size();


      System.out.println("\n--> Dataset size: " + endKey + "\n");
      for(int i = startKey; i < endKey; i++) {
         res256 = bPerf256.searchIO(bt256val.pop(), uniqueEntries,
                 verboseResults);
         res1024 = bPerf1024.searchIO(bt1024val.pop(), uniqueEntries,
                 verboseResults);
         res2048 = bPerf2048.searchIO(bt2048val.pop(), uniqueEntries,
                 verboseResults);

         if(res256[8] == 1) {found_cnt256++;}
         if(res1024[8] == 1) {found_cnt1024++;}
         if(res2048[8] == 1) {found_cnt2048++;}
      }

      System.out.println("Total pages for bt256 in the end: " +
              bt256.getTotalTreePages());
      System.out.println("Total pages for bt1024 in the end: " +
              bt1024.getTotalTreePages());
      System.out.println("Total pages for bt2048 in the end: " +
              bt2048.getTotalTreePages());

      // check result numbers
      if(found_cnt256 != totalKeys)
         {throw new Exception("BTree with page size: 256 failed to find all keys");}

      if(found_cnt1024 != totalKeys)
         {throw new Exception("BTree with page size: 1024 failed to find all keys");}

      if(found_cnt2048 != totalKeys)
         {throw new Exception("BTree with page size: 2048 failed to find all keys");}
   }

    /**
     * This test loads up a massive non-unique key list in
     * random order (10^5) onto trees of the following degrees:
     *
     *    - Page sizes: 256, 1024 (1Kb), 2048 (2Kb)
     *
     * with the following (Key, Value) settings:
     *
     *    - Satellite data size: 20 Bytes each entry
     *    - Key size: 8 bytes
     *
     * After insertion they are deleted in the same order as they were
     * put in.
     *
     * @throws Exception is thrown when an error is catch'ed in any of the operations performed.
     */
   @Test
   public void testMassRandomInsertionsWithDelete() throws Exception {
      uniqueEntries = false;
      verboseResults = false;
       recreateTree = true;

      LinkedList<Long> bt256val, bt1024val, bt2048val;


      // initialize the configuration
      btConf256 = new BPlusConfiguration(256);
      btConf1024 = new BPlusConfiguration(1024);
      btConf2048 = new BPlusConfiguration(2048);

      // set up the the counters for each tree
      bPerf256 = new BPlusTreePerformanceCounter(true);
      bPerf1024 = new BPlusTreePerformanceCounter(true);
      bPerf2048 = new BPlusTreePerformanceCounter(true);

      // finally setup the tree instances
      bt256 = new BPlusTree(btConf256, recreateTree ? "rw+" : "rw",
              "tree256.bin", bPerf256);
      bt1024 = new BPlusTree(btConf1024, recreateTree ? "rw+" : "rw",
              "tree1024.bin", bPerf1024);
       bt2048 = new BPlusTree(btConf2048, recreateTree ? "rw+" : "rw",
              "tree2048.bin", bPerf2048);

      // randomly add non-unique insertions
      bt256val = Utilities.fuzzyAddToTree(startKey, endKey,
              uniqueEntries, bt256);

      bt1024val = Utilities.fuzzyAddToTree(startKey, endKey,
              uniqueEntries, bt1024);
      bt2048val = Utilities.fuzzyAddToTree(startKey, endKey,
              uniqueEntries, bt2048);

      // our counters
      int found_cnt256 = 0;
      int found_cnt1024 = 0;
      int found_cnt2048 = 0;

      int[] res256, res1024, res2048;

      //System.out.println("\n--> Dataset size: " + endKey + "\n");

      for(int i = startKey; i < endKey; i++) {
         res256 = bPerf256.deleteIO(bt256val.pop(), true,
                 verboseResults);
         res1024 = bPerf1024.deleteIO(bt1024val.pop(), true,
                 verboseResults);
         res2048 = bPerf2048.deleteIO(bt2048val.pop(), true,
                 verboseResults);

         if(res256[8] == 1) {found_cnt256++;}
         if(res1024[8] == 1) {found_cnt1024++;}
         if(res2048[8] == 1) {found_cnt2048++;}
      }

      System.out.println("Total pages for bt256 in the end: " + bt256.getTotalTreePages());
      System.out.println("Total pages for bt1024 in the end: " + bt1024.getTotalTreePages());
      System.out.println("Total pages for bt2048 in the end: " + bt2048.getTotalTreePages());

      // check result numbers
      if(found_cnt256 != totalKeys)
         {throw new Exception("BTree with page size: 256 failed to delete all keys");}

      if(found_cnt1024 != totalKeys)
         {throw new Exception("BTree with page size: 1024 failed to delete all keys");}

      if(found_cnt2048 != totalKeys)
         {throw new Exception("BTree with page size: 2048 failed to delete all keys");}

   }

}
