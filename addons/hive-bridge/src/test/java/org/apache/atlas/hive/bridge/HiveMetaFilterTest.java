package org.apache.atlas.hive.bridge;

import com.google.common.collect.Sets;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;

/**
 * Created by A744013 on 6/17/16.
 */
public class HiveMetaFilterTest {

    @Test
    public void testDefaultConstructor() {
        HiveMetaFilter filter = new HiveMetaFilter();

        Assert.assertEquals(filter.filterDatabases(Arrays.asList("abc", "def", "ghi")).size(), 3);

        Assert.assertEquals(filter.filterTables("abc", Arrays.asList("tb1", "tb2")).size(), 2);
    }

    @Test
    public void testMatchNone() {
        HiveMetaFilter filter = new HiveMetaFilter(
                Sets.<Filter>newHashSet(Filters.MATCH_NONE),
                FilterUtils.getDefault());

        Assert.assertEquals(filter.filterDatabases(Arrays.asList("abc", "def", "ghi")).size(), 0);

        Assert.assertEquals(filter.filterTables("abc", Arrays.asList("tb1", "tb2")).size(), 2);
    }

    @Test
    public void testDbMatchNone() {
        HiveMetaFilter filter = new HiveMetaFilter(
                Sets.<Filter>newHashSet(Filters.MATCH_NONE),
                FilterUtils.getDefault());

        Assert.assertEquals(filter.filterDatabases(Arrays.asList("abc", "def", "ghi")).size(), 0);

        Assert.assertEquals(filter.filterTables("abc", Arrays.asList("tb1", "tb2")).size(), 2);
    }
}
