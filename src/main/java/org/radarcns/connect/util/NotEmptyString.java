/*
 * Copyright 2017 The Hyve and King's College London
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.connect.util;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class NotEmptyString implements ConfigDef.Validator {
    @Override
    public void ensureValid(String name, Object value) {
        if (value == null) {
            throw new ConfigException(name, null, "Null");
        }
        if (!(value instanceof String)) {
            throw new ConfigException(name, value, "Not a string");
        }
        if (((String)value).trim().isEmpty()) {
            throw new ConfigException(name, value, "String may not be empty");
        }
    }

    @Override
    public String toString() {
        return "Non-empty string";
    }
}
