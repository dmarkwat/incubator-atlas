package org.apache.atlas.hive.bridge;

import org.testng.annotations.Test;

import java.util.Set;

import static org.testng.Assert.assertEquals;

/**
 * Created by A744013 on 6/16/16.
 */
public class HiveBridgeOptionsTest {

    @Test
    public void testNoArgs() {
        HiveBridgeOptions options = new HiveBridgeOptions();

        Set<Filter> dbF = options.getFilter().getDatabaseFilters();

        assertEquals(dbF.size(), 1);
        assertEquals(dbF.iterator().next(), Filters.MATCH_ALL);

        Set<Filter> tableF = options.getFilter().getTableFilters();

        assertEquals(tableF.size(), 1);
        assertEquals(tableF.iterator().next(), Filters.MATCH_ALL);
    }

    @Test
    public void testEmptyArgs() {
        HiveBridgeOptions options = new HiveBridgeOptions(new String[]{});

        Set<Filter> dbF = options.getFilter().getDatabaseFilters();

        assertEquals(dbF.size(), 1);
        assertEquals(dbF.iterator().next(), Filters.MATCH_ALL);

        Set<Filter> tableF = options.getFilter().getTableFilters();

        assertEquals(tableF.size(), 1);
        assertEquals(tableF.iterator().next(), Filters.MATCH_ALL);
    }
}
