package org.apache.atlas.hive.bridge;

import com.google.common.collect.Sets;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;

/**
 * Created by A744013 on 6/17/16.
 */
public class MatchFilterTest {

    @Test
    public void testEmpty() {
        MatchFilter filter = new MatchFilter(Sets.<String>newHashSet());

        Assert.assertEquals(filter.filter(Arrays.asList("abc")).size(), 0);
    }

    @Test
    public void testNonEmpty() {
        MatchFilter filter = new MatchFilter(Sets.newHashSet("abc", "def"));

        Assert.assertEquals(filter.filter(Arrays.asList("abc", "def")).size(), 2);

        Assert.assertEquals(filter.filter(Arrays.asList("abc", "def", "ghi")).size(), 2);

        Assert.assertEquals(filter.filter(Arrays.asList("abc", "ghi")).size(), 1);

        Assert.assertEquals(filter.filter(Arrays.asList("ghi", "xyz")).size(), 0);
    }
}
