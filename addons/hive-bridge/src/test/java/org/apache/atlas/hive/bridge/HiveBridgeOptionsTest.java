package org.apache.atlas.hive.bridge;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Sets;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

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

        Map<String, Collection<Filter>> tableF = options.getFilter().getTableFilters();

        Collection<Filter> filter = tableF.get(null);

        assertEquals(tableF.size(), 0);
        assertEquals(filter, Sets.newHashSet(Filters.MATCH_ALL));
    }

    @Test
    public void testEmptyArgs() {
        HiveBridgeOptions options = new HiveBridgeOptions(new String[]{});

        Set<Filter> dbF = options.getFilter().getDatabaseFilters();

        assertEquals(dbF.size(), 1);
        assertEquals(dbF.iterator().next(), Filters.MATCH_ALL);

        Map<String, Collection<Filter>> tableF = options.getFilter().getTableFilters();

        Collection<Filter> filter = tableF.get(null);

        assertEquals(tableF.size(), 0);
        assertEquals(filter, Sets.newHashSet(Filters.MATCH_ALL));
    }

    @Test
    public void testTableFilter() {
        HiveBridgeOptions options = new HiveBridgeOptions(new String[]{"--tables", "db1.tb1,db2.tb2,db3.tb3"});

        HiveMetaFilter hmf = options.getFilter();

        Set<Filter> dbF = hmf.getDatabaseFilters();

        assertEquals(dbF.size(), 1);
        assertEquals(dbF.iterator().next(), Filters.MATCH_ALL);

        Map<String, Collection<Filter>> tableF = options.getFilter().getTableFilters();

        assertEquals(tableF.size(), 3);
        assertTrue(tableF.containsKey("db1"));
        assertTrue(tableF.containsKey("db2"));
        assertTrue(tableF.containsKey("db3"));

        assertEquals(tableF.get("db1"), Sets.newHashSet(new MatchFilter("tb1")));
        assertEquals(tableF.get("db2"), Sets.newHashSet(new MatchFilter("tb2")));
        assertEquals(tableF.get("db3"), Sets.newHashSet(new MatchFilter("tb3")));
    }

    @Test
    public void testDbFilter() {
        HiveBridgeOptions options = new HiveBridgeOptions(new String[]{"--databases", "db1,db2"});

        HiveMetaFilter hmf = options.getFilter();

        Set<Filter> dbF = hmf.getDatabaseFilters();

        assertEquals(dbF.size(), 2);
        assertTrue(dbF.iterator().next() instanceof MatchFilter);

        Iterator<Filter> iter = dbF.iterator();

        MatchFilter matcher = (MatchFilter) iter.next();
        assertEquals(matcher.getAccepted(), Sets.newHashSet("db1"));

        matcher = (MatchFilter) iter.next();
        assertEquals(matcher.getAccepted(), Sets.newHashSet("db2"));

        Map<String, Collection<Filter>> tableF = options.getFilter().getTableFilters();

        Collection<Filter> filter = tableF.get(null);

        assertEquals(tableF.size(), 0);
        assertEquals(filter, Sets.newHashSet(Filters.MATCH_ALL));
    }
}
