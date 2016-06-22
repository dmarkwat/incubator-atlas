package org.apache.atlas.hive.bridge;

import java.util.List;

/**
 * Created by A744013 on 6/16/16.
 */
public interface Filter {

    List<String> filter(List<String> toFilter);
}
