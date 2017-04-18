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

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Map;
import org.junit.Test;

public class UtilityTest {
    @Test
    public void configToString() {
        Map<String, String> config = Collections.singletonMap("something", "other");
        String result = Utility.convertConfigToString(config);
        String[] lines = result.split("\\r?\\n");
        assertEquals(2, lines.length);
        assertEquals("\tsomething: other", lines[1]);
    }
}
