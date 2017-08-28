Pehr Collins (Hash Buckets/Index/Scan)
12/2/2007

A hash bucket is a hashing data structure similar to what is used by some databases.

Overall Status:

I implemented the stubbed methods in HashBucketPage.Java, HashIndex.Java, and HashScan.java.  I started
with HashBucketPage which was fairly straightforward.  Then I implemented HashIndex.Java which took the
bulk of my time.  Finally I implemented HashScan.java which required some debugging, but was not 
overly difficult.  On the HashScan, the comment says it should throw an IllegalArgumentException if 
the scan has no more entries.  It didn't appear that the test code expected an exception in this case
(it blew up) so I just returned null instead.
