![build](https://travis-ci.org/andylamp/BPlusTree.svg?branch=master)

# A Purely *On-Disk*  Implementation of a B+ Tree


After quite a few hours that included developing the thing as well as testing it here is
a (fully) functional implementation of a B+ Tree data structure purely ***on the disk***. 
This was developed mostly for educational reasons that stemmed from the fact that I could not
 find a B+ Tree implementation that met the following:
 
 * Purely Disk based implementation
 * Used strict paging which the user could tune (256k, 1K, 4K etc)
 * Was a (Key, Value) storage not just for Keys (B-Tree)
 * Implemented **deleting** from the data structure
 * Supported duplicate entries (if required)
 * was adequately *tested* (so that I know it would work)
 
I came about needing to implement a B+ Tree for a side project of mine... and I was really
surprised to see that there were no implementations that met the above requirements; not only
that but I was not able to find a reference code/pseudocode that had a working **delete**
implementation and support for duplicate keys. To my dismay even my trusty (and beloved) 
CLRS didn't include an implementation of a B+ Tree but only for the B-Tree with no duplicates 
and **delete** pseudocode (well, one could say that they gave the steps...but that's 
not the point).

It has to be noted that I *could* find some B+ Tree implementations that had delete and duplicate
 key support, mainly in open-source database projects. This unfortunately meant that these
 implementations  were deeply integrated into their parent projects and as a result they were
 optimized for their particular use-cases. Finally the code bases where significantly larger
 (hence making the code reading/understanding much harder than it should be!).

So I went about to implement mine (cool stuff, many hours of head scratching were involved!) 
while also putting effort in creating this "tuned" down version which mainly cuts down on 
features for simplicity, enhanced code clarity and clutter reduction.


# Ease of use features
  
  
 As I said above this project was done mainly to create a working example of a B+ Tree data
 structure purely on the disk so the code is well commented (I think) and can be understood
 easily; that said... we have some "delicacies" that make working and testing this project
 a bit easier, which are outlined below
 
 * it uses maven, so most modern IDE's can import it without hassle...
 * it requires jdk v8 (for some parts, change them to have backwards support)
 * it uses a dedicated tester as well as JUnit tests
 * has an interactive menu that the user can individually perform the operations
 
 
# Implementation details

## Insertions

For insertions we use a modified version of the method provided by CLRS with modifications to
support duplicate keys (which will be covered below). Complexity is not altered from the
usual case and we require one pass down the tree as well.

## Searching

This is assumed to be the most common operation on the B+ Tree; which support two modes of
searching:

* Singular Key searches
* Range Queries

Here two distinct functions are used to cover these two cases:

* **searchKey** is used for singular value searches (unique or not)
* **rangeSearch** is used for performing range queries

Both of these methods require only one pass over the tree to find the results (if any). Additionally, since
we store the keys in a sorted order we can exploit binary search to reduce the total node lookup time *significantly*.
This is done along with a slight modification to the search algorithm to return the lower bound instead of failure.

## Deletes

We again use one pass down the tree deletes but this is a bit more complex than the other
two operations... it again requires only one pass to delete the key (if found) but as it
descends down the tree it performs any redistribution/merging that needs to happen in 
order to keep our data structure valid.

## Handling of duplicate keys

In order to avoid hurting the search performance functionality which is (assumed to be) 
the most common operation in a B+ Tree data structure the following scheme was 
implemented to support duplicate keys (at the cost of a bit more page reads). 

The tree only actually indexes singular keys but each key has an *overflow* pointer 
which leads to its *overflow page* that has all of the duplicate entries stored as a 
linked list. If needed, multiple trailing *overflow* pages per key are created to 
accommodate for these extra insertions should they exceed the capacity of one page. 
The downside is that we use a bit more space per page as well as reads in order to 
read these overflow pages.

## Page lookup table

To avoid moving around things too much we keep each page into a *free page pool* that has
all of the available pages so far; this in turn let's us create an index very fast
without having to pay costly reads if we wanted to have a clustered tree (although
we again use more space, usually).

# License

This work is licensed under the GPLv3 license... so that others can view, modify and expand this,
should they so desire.

# Final words

Hopefully I'll create a GitHub page for this... where I explain my implementation a bit 
more but until then this will suffice! Oh and I really hope this implementation is clear
 and concise enough so that it can make the notions of B+ Trees crystal clear!
