package org.apache.atlas.hive.bridge;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/**
 * Created by A744013 on 6/16/16.
 */
public class MatchNotFilter extends MatchFilter implements Filter {

    public MatchNotFilter(String accepted) {
        super(accepted);
    }

    public MatchNotFilter(Set<String> accepted) {
        super(accepted);
    }

    @Override
    public List<String> filter(List<String> toFilter) {
        List<String> filtered = super.filter(toFilter);
        return Lists.newArrayList(
                Sets.difference(Sets.newHashSet(toFilter), Sets.newHashSet(filtered)));
    }
}
