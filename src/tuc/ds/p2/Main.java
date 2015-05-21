package tuc.ds.p2;

import tuc.ds.bptree.BPlusConfiguration;
import tuc.ds.bptree.BPlusTree;
import tuc.ds.bptree.SearchResult;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        boolean unique = true;
        BPlusConfiguration btconf = new BPlusConfiguration();
        BPlusTree bt = new BPlusTree(btconf, "rw");

        bt.printCurrentConfiguration();

//        for(int i = 20; i < 50; i++) {
//            bt.insertKey(i, "asdfasdfas", unique);
//        }
//
//        for(int i = 0; i < 5; i++) {
//            bt.insertKey(23, "asdfasdfas", !unique);
//        }
        //bt.insertKey(45, "asdfasdfas");

        /*
        bt.insertKey(1, "asdfasdfas");
        bt.insertKey(10, "asdfasdfas");

        bt.printTree();

        bt.insertKey(5, "asdfasdfas");
        bt.insertKey(20, "asdfasdfas");

        bt.printTree();
        bt.insertKey(25, "asdfasdfas");

        bt.printTree();

        bt.insertKey(2, "asdfasdfas");

        bt.printTree();

        bt.insertKey(4, "asdfasdfas");

        bt.printTree();

        bt.insertKey(3, "asdfasdfas");

        bt.insertKey(30, "asdfasdfas");



        bt.printTree();

        bt.insertKey(29, "asdfasdfas");


        bt.insertKey(35, "asdfasdfas");

        bt.insertKey(40, "asdfasdfas");


        bt.insertKey(36, "asdfasdfas");

        bt.insertKey(42, "asdfasdfas");


        bt.insertKey(38, "asdfasdfas");

        bt.insertKey(43, "asdfasdfas");


        bt.insertKey(50, "asdfasdfas");

        bt.insertKey(51, "asdfasdfas");
        */



        //bt.insertKey(82, "asdfasdfas");
        //bt.insertKey(83, "asdfasdfas");
        //bt.insertKey(84, "asdfasdfas");

        //bt.printTree();
        SearchResult s = bt.searchKey(23, false);
        //if(s == null)
        //    {System.out.println("Key is totally outside the range of the tree");}
        if(s.isFound())
            {System.out.println("Key found");}
        else
            {System.out.println("Key NOT found");}

        //RangeResult r = bt.rangeSearch(10, 15);

        bt.commitTree();

        /*
        byte[] data = new byte[10];

        for(int i = 0; i < data.length; i++) {data[i] = (byte)i;}

        String s1 = Arrays.toString(data);

        String s2 = "asdfg";

        byte[] barrray = s2.getBytes(StandardCharsets.UTF_8);

        String s3 = new String(barrray);
        System.out.println("s1: " + s1);
        System.out.println("s2: " + s2);
        System.out.println("s3: " + s3);
        System.out.println("byte array length: " + barrray.length);

        ArrayList<TreeNodeEntry<Long>> l = new ArrayList<>();

        for(int i = 0; i < 10; i++) {
            l.add(new TreeNodeEntry<Long>((long)10-i, (long)i));
            l.get(i).printPair();
        }

        Collections.sort(l);

        System.out.println("AFTER SORTING \n\n\n");

        for(TreeNodeEntry<Long> e : l)
            {e.printPair();}
        */
    }
}
