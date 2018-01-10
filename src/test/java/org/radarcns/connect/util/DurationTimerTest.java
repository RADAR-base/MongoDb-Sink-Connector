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

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class DurationTimerTest {
    @Test
    public void duration() throws Exception {
        DurationTimer timer = new DurationTimer();
        assertTrue(timer.duration() < 0.01d);
        Thread.sleep(100L);
        assertTrue(timer.duration() > 0.1d);
        assertTrue(timer.duration() > 0.1d);
        timer.reset();
        assertTrue(timer.duration() < 0.01d);
    }
}