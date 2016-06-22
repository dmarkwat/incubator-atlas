package org.apache.atlas.hive.bridge;

import com.google.common.collect.Sets;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;

/**
 * Created by A744013 on 6/17/16.
 */
public class MatchNotFilterTest {

    @Test
    public void testEmpty() {
        MatchNotFilter filter = new MatchNotFilter(Sets.<String>newHashSet());

        Assert.assertEquals(filter.filter(Arrays.asList("abc")).size(), 1);
    }

    @Test
    public void testNonEmpty() {
        MatchNotFilter filter = new MatchNotFilter(Sets.newHashSet("abc", "def"));

        Assert.assertEquals(filter.filter(Arrays.asList("abc", "def")).size(), 0);

        Assert.assertEquals(filter.filter(Arrays.asList("abc", "def", "ghi")).size(), 1);

        Assert.assertEquals(filter.filter(Arrays.asList("abc", "ghi")).size(), 1);

        Assert.assertEquals(filter.filter(Arrays.asList("ghi", "xyz")).size(), 2);
    }
}
