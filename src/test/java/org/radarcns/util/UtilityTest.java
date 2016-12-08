package org.radarcns.util;

import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;
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
    public void emptyStringToSet() throws Exception {
        assertThat(Utility.stringToSet(""), is(empty()));
    }
    @Test
    public void singletonStringToSet() throws Exception {
        assertThat(Utility.stringToSet("something"), containsInAnyOrder("something"));
    }

    @Test
    public void multipleStringToSet() throws Exception {
        assertThat(Utility.stringToSet("prop1,prop2,prop3"), containsInAnyOrder("prop1", "prop2", "prop3"));
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
}
