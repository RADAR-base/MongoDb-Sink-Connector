package org.radarcns.util;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class NotEmptyString implements ConfigDef.Validator {
    @Override
    public void ensureValid(String name, Object value) {
        if (((String)value).trim().isEmpty()) {
            throw new ConfigException(name, value, "String may not be empty");
        }
    }
}
