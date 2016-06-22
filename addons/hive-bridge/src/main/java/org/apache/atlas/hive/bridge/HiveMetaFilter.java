package org.apache.atlas.hive.bridge;

import com.google.common.collect.Sets;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.map.DefaultedMap;
import org.apache.commons.collections4.map.UnmodifiableMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.apache.commons.collections4.set.UnmodifiableSet;

import java.util.*;

/**
 * Created by A744013 on 6/16/16.
 */
public class HiveMetaFilter {

    private final Set<Filter> databaseFilters;
    private final Map<String, Collection<Filter>> tableFilters;

    public HiveMetaFilter() {
        this(Sets.<Filter>newHashSet(Filters.MATCH_ALL),
                DefaultedMap.defaultedMap(
                        new HashMap<String, Collection<Filter>>(),
                        Sets.<Filter>newHashSet(Filters.MATCH_ALL)));
    }

    public HiveMetaFilter(Set<Filter> databaseFilters, Map<String, Collection<Filter>> tableFilters) {
        this.databaseFilters = databaseFilters;
        this.tableFilters = tableFilters;
    }

    public Set<Filter> getDatabaseFilters() {
        return UnmodifiableSet.unmodifiableSet(databaseFilters);
    }

    public Map<String, Collection<Filter>> getTableFilters() {
        return UnmodifiableMap.unmodifiableMap(tableFilters);
    }

    public List<String> filterDatabases(List<String> databases) {
        List<String> filtered = databases;
        for (Filter filter : databaseFilters) {
            filtered = filter.filter(filtered);
        }
        return filtered;
    }

    public List<String> filterTables(String db, List<String> tables) {
        List<String> filtered = tables;
        for (Filter filter : tableFilters.get(db)) {
            filtered = filter.filter(filtered);
        }
        return filtered;
    }

    public static class Builder {

        private final Set<Filter> databaseFilters;
        private final MultiValuedMap<String, Filter> tableFilters;

        public Builder() {
            databaseFilters = new HashSet<>();
            tableFilters = new HashSetValuedHashMap<>();
        }

        public Builder addDatabaseFilter(Filter filter) {
            databaseFilters.add(filter);
            return this;
        }

        public Builder addTableFilter(String db, Filter filter) {
            tableFilters.put(db, filter);
            return this;
        }

        public HiveMetaFilter build() {
            Set<Filter> set = Sets.newHashSet(databaseFilters);
            if (set.isEmpty())
                set.add(Filters.MATCH_ALL);

            Map<String, Collection<Filter>> map = DefaultedMap.defaultedMap(tableFilters.asMap(),
                    Sets.<Filter>newHashSet(Filters.MATCH_ALL));

            return new HiveMetaFilter(set, map);
        }
    }
}
