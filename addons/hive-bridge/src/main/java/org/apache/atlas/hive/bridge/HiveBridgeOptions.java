package org.apache.atlas.hive.bridge;

import com.google.common.base.Joiner;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by A744013 on 6/16/16.
 */
public class HiveBridgeOptions {

    private static final Logger LOG = LoggerFactory.getLogger(HiveBridgeOptions.class);

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

        OptionSet options = parser.parse(argv);
        if (options.has(TABLE_OPT)) {
            List<String> values = options.valuesOf(TABLE_OPT);
            LOG.info("--tables: {}", Joiner.on(',').join(values));
            for (String table : values) {
                String[] parts = StringUtils.split(table, ".", 2);
                Validate.isTrue(parts.length > 1, String.format("Table, '%s', does not have a '.' separator", table));
                builder.addTableFilter(parts[0], new MatchFilter(parts[1]));
            }
        }

        if (options.has(DB_OPT)) {
            List<String> values = options.valuesOf(DB_OPT);
            LOG.info("--databases: {}", Joiner.on(',').join(values));
            for (String db : values) {
                Validate.isTrue(StringUtils.countMatches(db, ".") == 0);
                builder.addDatabaseFilter(new MatchFilter(db));
            }
        }

        if (options.has(NOT_TABLE_OPT)) {
            List<String> values = options.valuesOf(NOT_TABLE_OPT);
            LOG.info("--not-tables: {}", Joiner.on(',').join(values));
            for (String table : values) {
                String[] parts = StringUtils.split(table, ".", 2);
                Validate.isTrue(parts.length > 1, String.format("Table, '%s', does not have a '.' separator", table));
                builder.addTableFilter(parts[0], new MatchNotFilter(parts[1]));
            }
        }

        if (options.has(NOT_DB_OPT)) {
            List<String> values = options.valuesOf(NOT_DB_OPT);
            LOG.info("--not-databases: {}", Joiner.on(',').join(values));
            for (String db : values) {
                Validate.isTrue(StringUtils.countMatches(db, ".") == 0);
                builder.addDatabaseFilter(new MatchNotFilter(db));
            }
        }

        this.filter = builder.build();
    }

    public HiveMetaFilter getFilter() {
        return filter;
    }

    @Override
    public String toString() {
        return "HiveBridgeOptions{" +
                "filter=" + filter +
                '}';
    }
}
