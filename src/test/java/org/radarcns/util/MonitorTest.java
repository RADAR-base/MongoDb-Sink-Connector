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

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MonitorTest {
    private Logger mockLogger;
    private AtomicInteger count;

    @Before
    public void runBeforeTest() {
        mockLogger = mock(Logger.class);
        count = new AtomicInteger(0);
    }

    @Test
    public void runWithoutBuffer() throws Exception {
        Monitor monitor = new Monitor(mockLogger, count, "test");
        monitor.run();
        verify(mockLogger).info("{} {}", 0, "test");
        assertEquals(0, count.get());
        count.set(100);

        monitor.run();
        verify(mockLogger).info("{} {}", 100, "test");
        assertEquals(0, count.get());
    }

    @Test
    public void runWithBuffer() throws Exception {
        Collection<String> buffer = new ArrayList<>();
        buffer.add("one");

        Monitor monitor = new Monitor(mockLogger, count, "test", buffer);
        monitor.run();
        verify(mockLogger).info("{} {} {} records need to be processed.", 0, "test", 1);
        assertEquals(0, count.get());
        count.set(100);
        buffer.clear();

        monitor.run();
        verify(mockLogger).info("{} {} {} records need to be processed.", 100, "test", 0);
        assertEquals(0, count.get());
    }
}