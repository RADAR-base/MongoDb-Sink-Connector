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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;

public class UtilityTest {
    @Test
    public void emptyKeyListToString() throws Exception {
        assertEquals("", Utility.keyListToString(Collections.emptyMap()));
    }

    @Test
    public void singletonKeyListToString() throws Exception {
        Map<String, String> testMap = Collections.singletonMap("something", "or other");
        assertEquals("something", Utility.keyListToString(testMap));
    }

    @Test
    public void multipleKeyListToString() throws Exception {
        // use a SortedMap
        Map<String, String> testMap = new TreeMap<>();
        testMap.put("prop1", "something");
        testMap.put("prop2", "something");
        testMap.put("prop3", "something");
        assertEquals("prop1,prop2,prop3", Utility.keyListToString(testMap));
    }

    @Test
    public void getDefaultInt() throws Exception {
        assertEquals(10, Utility.getInt(Collections.emptyMap(), "something", 10));
    }

    @Test
    public void getStatedInt() throws Exception {
        Map<String, String> testMap =Collections.singletonMap("something", "30");
        assertEquals(30, Utility.getInt(testMap, "something", 10));
    }

    @Test
    public void getMalformedInt() throws Exception {
        Map<String, String> testMap =Collections.singletonMap("something", "some30");
        assertEquals(10, Utility.getInt(testMap, "something", 10));
    }

    @Test
    public void intervalKeyToMongo() {
        SchemaBuilder sb = SchemaBuilder.struct();
        sb.field("userID", Schema.STRING_SCHEMA);
        sb.field("sourceID", Schema.STRING_SCHEMA);
        sb.field("start", Schema.INT64_SCHEMA);
        sb.field("end", Schema.INT64_SCHEMA);
        Schema schema = sb.schema();
        Struct s = new Struct(schema);
        s.put("userID", "myUser");
        s.put("sourceID", "mySource");
        s.put("start", 1000L);
        s.put("end", 2000L);
        assertEquals("myUser-mySource-1000-2000", Utility.intervalKeyToMongoKey(s));
    }

    @Test
    public void testParseArrayConfig() {
        Map<String, String> testMap = new TreeMap<>();
        testMap.put("testValues", "prop2,prop3");
        testMap.put("prop1", "something1");
        testMap.put("prop2", "something2");
        testMap.put("prop3", "something3");
        Map<String, String> result = Utility.parseArrayConfig(testMap, "testValues");
        assertThat(result, hasEntry("prop2", "something2"));
        assertThat(result, hasEntry("prop3", "something3"));
        assertThat(result, not(hasKey("prop1")));
        assertThat(result, not(hasKey("testValues")));
    }

    @Test
    public void testNonExistingParseArrayConfig() {
        Map<String, String> testMap = new TreeMap<>();
        testMap.put("testValues", "prop2,prop3,prop4");
        testMap.put("prop1", "something1");
        testMap.put("prop2", "something2");
        testMap.put("prop3", "something3");
        assertEquals(null, Utility.parseArrayConfig(testMap, "testValues"));
    }

    @Test
    public void testEmptyPropertyParseArrayConfig() {
        Map<String, String> testMap = new TreeMap<>();
        testMap.put("testValues", "prop2,prop3");
        testMap.put("prop1", "something1");
        testMap.put("prop2", "something2");
        testMap.put("prop3", "");
        assertEquals(null, Utility.parseArrayConfig(testMap, "testValues"));
    }

    @Test
    public void testEmptyValueParseArrayConfig() {
        Map<String, String> testMap = new TreeMap<>();
        testMap.put("testValues", "");
        testMap.put("prop1", "something1");
        testMap.put("prop2", "something2");
        testMap.put("prop3", "something3");
        assertThat(Utility.parseArrayConfig(testMap, "testValues"), is(Collections.emptyMap()));
    }

    @Test
    public void testEmptyLoadClasses() {
        assertThat(Utility.loadRecordConverters(getClass().getClassLoader(), ""), empty());
    }

    @Test
    public void testKnownLoadClasses() {
        String classString = "org.radarcns.serialization.DoubleAggregatedRecordConverter,"
                + "org.radarcns.serialization.AggregatedAccelerationRecordConverter";
        ClassLoader loader = getClass().getClassLoader();
        assertThat(Utility.loadRecordConverters(loader, classString), hasSize(2));
    }

    @Test
    public void testUnknownLoadClasses() {
        String classString = "org.radarcns.serialization.DoubleAggregatedRecordConverter,"
                + "org.radarcns.serialization.UNKNOWN";
        ClassLoader loader = getClass().getClassLoader();
        assertThat(Utility.loadRecordConverters(loader, classString), hasSize(1));
    }
}
