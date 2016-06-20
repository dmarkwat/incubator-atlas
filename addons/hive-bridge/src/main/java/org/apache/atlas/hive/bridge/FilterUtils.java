package org.apache.atlas.hive.bridge;

import com.google.common.collect.Sets;
import org.apache.commons.collections4.map.DefaultedMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by A744013 on 6/17/16.
 */
public class FilterUtils {

    private FilterUtils() {
    }

    public static Map<String, Collection<Filter>> getDefault() {
        return DefaultedMap.defaultedMap(new HashMap<String, Collection<Filter>>(),
                Sets.<Filter>newHashSet(Filters.MATCH_ALL));
    }
}
