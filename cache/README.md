Caches with changing values
===========================

Key-value cache implementations supporting cache-entries with changing values. Including support for cache-entry prioritization that changes as cache-values changes. Thread-safe.

Useful for e.g. database-caches. E.g.

* Query-result cache, where you do not necessarily want to invalidate the cache-entry, just because e.g. a records was added that has to be included in the cached query-result. Maybe the query-result is more important to keep in cache the larger the result-set gets<br/>
See [StringStringOptimisticLockingDBWithVersionCache](src/main/java/ae/teletronics/cache/examples/dbversioncache/StringStringOptimisticLockingDBWithVersionCache.java) and its test [StringStringOptimisticLockingDBWithVersionCacheTest](src/test/java/ae/teletronics/cache/examples/dbversioncache/StringStringOptimisticLockingDBWithVersionCacheTest.java) for inspiration 
* Using version-control and updates to the cache does not necessarily arrive at the cache in the same order as they were carried out against the cache<br/>
See [StringStringOptimisticLockingDBWithKeyStartsWithCache](src/main/java/ae/teletronics/cache/examples/dbversioncache/StringStringOptimisticLockingDBWithKeyStartsWithCache.java) and its test [StringStringOptimisticLockingDBWithVersionCacheTest](src/test/java/ae/teletronics/cache/examples/dbversioncache/StringStringOptimisticLockingDBWithKeyStartsWithCacheTest.java) for inspiration

## Using the binaries (via maven dependencies)
```xml
    <dependency>
        <groupId>ae.teletronics.toolbox</groupId>
        <artifactId>cache</artifactId>
        <version>0.8</version>
    </dependency>
```
