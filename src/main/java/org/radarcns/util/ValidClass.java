/*
 *  Copyright 2016 Kings College London and The Hyve
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

package org.radarcns.util;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

/**
 * Validate a class name.
 */
public class ValidClass implements ConfigDef.Validator {
    private final Class<?> superClass;

    private ValidClass(Class<?> superClass) {
        this.superClass = superClass;
    }

    /** Ensures that classes are subclass of the given class and that they are instantiable. */
    public static ValidClass isSubclassOf(Class<?> cls) {
        if (cls == null) {
            throw new NullPointerException("Class name may not be null");
        }
        return new ValidClass(cls);
    }

    @Override
    public void ensureValid(String name, Object o) {
        Class<?> cls = (Class<?>) o;
        if (!superClass.isAssignableFrom(cls)) {
            throw new ConfigException(name, o,
                    "Class " + o + " must be subclass of " + superClass.getName());
        }
        try {
            cls.newInstance();
        } catch (InstantiationException e) {
            throw new ConfigException(name, o, "Class " + o + " must be instantiable: " + e);
        } catch (IllegalAccessException e) {
            throw new ConfigException(name, o, "Class " + o + " must be accessible: " + e);
        }
    }
}
