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

package org.radarcns.util;

import static org.junit.Assert.assertNotNull;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;
import org.omg.CORBA.Object;

public class ValidClassTest {
    @Test
    public void isSubclassOf() throws Exception {
        ValidClass validator = ValidClass.isSubclassOf(Object.class);
        assertNotNull(validator);
    }

    @Test(expected = IllegalArgumentException.class)
    public void isSubclassOfNull() throws Exception {
        ValidClass.isSubclassOf(null);
    }

    @Test
    public void ensureValid() throws Exception {
        ConfigDef.Validator validator = ValidClass.isSubclassOf(A.class);
        // same class
        validator.ensureValid("myname", A.class);
        // anonymous subclass
        validator.ensureValid("myname", B.class);
    }

    @Test(expected = ConfigException.class)
    public void ensureValidNotSubclass() {
        ConfigDef.Validator validator = ValidClass.isSubclassOf(A.class);
        validator.ensureValid("myname", C.class);

    }

    @Test(expected = ConfigException.class)
    public void ensureValidNotInstantiable() {
        // not instantiable
        ConfigDef.Validator validator = ValidClass.isSubclassOf(D.class);
        validator.ensureValid("myname", D.class);
    }

    @Test(expected = ConfigException.class)
    public void ensureValidNotAccessible() {
        // not instantiable
        ConfigDef.Validator validator = ValidClass.isSubclassOf(A.class);
        validator.ensureValid("myname", E.class);
    }

    static class A {}

    static class B extends A {}

    static class C {}

    // not instantiable
    static class D {
        private D() {}
    }

    private static class E extends A {}
}