package org.apache.atlas.hive.bridge;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by A744013 on 6/16/16.
 */
public class HiveBridgeOptions {

    private static final OptionParser parser = new OptionParser();
    private static final OptionSpec<String> TABLE_OPT;
    private static final OptionSpec<String> DB_OPT;
    private static final OptionSpec<String> NOT_TABLE_OPT;
    private static final OptionSpec<String> NOT_DB_OPT;

    static {
        TABLE_OPT = parser.accepts("tables", "A comma-separated list of fully-qualified table names")
                .withRequiredArg()
                .withValuesSeparatedBy(',');
        DB_OPT = parser.accepts("databases", "A comma-separated list of database names")
                .withRequiredArg()
                .withValuesSeparatedBy(',');
        NOT_TABLE_OPT = parser.accepts("not-tables", "A comma-separated list of fully-qualified tables names to exclude")
                .withRequiredArg()
                .withValuesSeparatedBy(',');
        NOT_DB_OPT = parser.accepts("not-databases", "A comma-separated list of database names to exclude")
                .withRequiredArg()
                .withValuesSeparatedBy(',');
    }

    private final HiveMetaFilter filter;

    public HiveBridgeOptions() {
        this.filter = new HiveMetaFilter();
    }

    public HiveBridgeOptions(String[] argv) {
        HiveMetaFilter.Builder builder = new HiveMetaFilter.Builder();

        Set<Filter> dbFilters = new HashSet<>();

        OptionSet options = parser.parse(argv);
        if (options.has(TABLE_OPT)) {
            for (String table : options.valuesOf(TABLE_OPT)) {
                String[] parts = StringUtils.split(table, ".", 2);
                Validate.isTrue(parts.length > 1, String.format("Table, '%s', does not have a '.' separator", table));
                builder.addTableFilter(parts[0], new MatchFilter(parts[1]));
            }
        }

        if (options.has(DB_OPT)) {
            for (String db : options.valuesOf(DB_OPT)) {
                Validate.isTrue(StringUtils.countMatches(db, ".") == 0);
                builder.addDatabaseFilter(new MatchFilter(db));
            }
        }

        if (options.has(NOT_TABLE_OPT)) {
            for (String table : options.valuesOf(TABLE_OPT)) {
                String[] parts = StringUtils.split(table, ".", 2);
                Validate.isTrue(parts.length > 1, String.format("Table, '%s', does not have a '.' separator", table));
                builder.addTableFilter(parts[0], new MatchNotFilter(parts[1]));
            }
        }

        if (options.has(NOT_DB_OPT)) {
            for (String db : options.valuesOf(DB_OPT)) {
                Validate.isTrue(StringUtils.countMatches(db, ".") == 0);
                builder.addDatabaseFilter(new MatchNotFilter(db));
            }
        }

        this.filter = builder.build();
    }

    public HiveMetaFilter getFilter() {
        return filter;
    }
}
