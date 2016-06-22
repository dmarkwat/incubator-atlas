package org.apache.atlas.hive.bridge;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by A744013 on 6/16/16.
 */
public enum Filters implements Filter {
    MATCH_ALL {
        @Override
        public List<String> filter(List<String> toFilter) {
            return Lists.newArrayList(toFilter);
        }

        @Override
        public String toString() {
            return "MATCH_ALL";
        }
    }, MATCH_NONE {
        @Override
        public List<String> filter(List<String> toFilter) {
            return new ArrayList<>();
        }

        @Override
        public String toString() {
            return "MATCH_NONE";
        }
    };
}
