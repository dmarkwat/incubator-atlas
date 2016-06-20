package org.apache.atlas.hive.bridge;

import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by A744013 on 6/16/16.
 */
public class MatchFilter implements Filter {

    private final Set<String> accepted;

    public MatchFilter(String accepted) {
        this.accepted = Sets.newHashSet(accepted);
    }

    public MatchFilter(Set<String> accepted) {
        this.accepted = accepted;
    }

    public Set<String> getAccepted() {
        return accepted;
    }

    @Override
    public List<String> filter(List<String> toFilter) {
        List<String> filtered = new ArrayList<>();
        for (String db : toFilter) {
            if (accepted.contains(db))
                filtered.add(db);
        }
        return filtered;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MatchFilter that = (MatchFilter) o;

        return CollectionUtils.isEqualCollection(accepted, that.accepted);

    }

    @Override
    public int hashCode() {
        return accepted.hashCode();
    }
}
