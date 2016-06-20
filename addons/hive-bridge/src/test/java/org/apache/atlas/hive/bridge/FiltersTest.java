package org.apache.atlas.hive.bridge;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;

/**
 * Created by A744013 on 6/17/16.
 */
public class FiltersTest {

    @Test
    public void testMatchAll() {
        Filter filter = Filters.MATCH_ALL;

        Assert.assertEquals(filter.filter(Arrays.asList("abc", "def")).size(), 2);
        Assert.assertEquals(filter.filter(Arrays.<String>asList()).size(), 0);
        Assert.assertEquals(filter.filter(Arrays.asList("abc")).size(), 1);
    }

    @Test
    public void testMatchNone() {
        Filter filter = Filters.MATCH_NONE;

        Assert.assertEquals(filter.filter(Arrays.asList("abc", "def")).size(), 0);
        Assert.assertEquals(filter.filter(Arrays.<String>asList()).size(), 0);
        Assert.assertEquals(filter.filter(Arrays.asList("abc")).size(), 0);
    }
}
