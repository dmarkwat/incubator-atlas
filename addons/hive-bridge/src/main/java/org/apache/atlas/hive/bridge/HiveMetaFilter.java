package org.apache.atlas.hive.bridge;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections.set.UnmodifiableSet;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

/**
 * Created by A744013 on 6/16/16.
 */
public class HiveMetaFilter {

    private final Set<Filter> databaseFilters;
    private final Set<Filter> tableFilters;

    public HiveMetaFilter() {
        this(Sets.<Filter>newHashSet(Filters.MATCH_ALL), Sets.<Filter>newHashSet(Filters.MATCH_ALL));
    }

    public HiveMetaFilter(Set<Filter> databaseFilters, Set<Filter> tableFilters) {
        this.databaseFilters = databaseFilters;
        this.tableFilters = tableFilters;
    }

    public Set<Filter> getDatabaseFilters() {
        return UnmodifiableSet.decorate(databaseFilters);
    }

    public Set<Filter> getTableFilters() {
        return UnmodifiableSet.decorate(tableFilters);
    }

    public List<String> filterDatabases(List<String> databases) {
        List<String> filtered = databases;
        for (Filter filter : databaseFilters) {
            filtered = filter.filter(filtered);
        }
        return filtered;
    }

    public List<String> filterTables(String db, List<String> tables) {
        final String dbRef = db;
        List<String> filtered = Lists.transform(
                tables,
                new Function<String, String>() {
                    @Nullable
                    @Override
                    public String apply(@Nullable String s) {
                        return s == null ? s : String.format("%s.%s", dbRef, s);
                    }
                });

        for (Filter filter : tableFilters) {
            filtered = filter.filter(filtered);
        }
        return filtered;
    }
}
