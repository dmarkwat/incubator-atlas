package org.apache.atlas.hive.bridge;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by A744013 on 6/16/16.
 */
public class MatchFilter implements Filter {

    private final Set<String> accepted;

    public MatchFilter(Set<String> accepted) {
        this.accepted = accepted;
    }

    public Set<String> getAccepted() {
        return accepted;
    }

    @Override
    public List<String> filter(List<String> toFilter) {
        List<String> filtered = new ArrayList<>();
        for (String db : accepted) {
            if (accepted.contains(db))
                filtered.add(db);
        }
        return filtered;
    }
}
