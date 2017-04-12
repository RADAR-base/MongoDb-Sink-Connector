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

/**
 * Measure the duration of an action. Not thread-safe.
 */
public class DurationTimer {
    private long nanoTime;

    public DurationTimer() {
        reset();
    }

    /** Reset the timer to zero. */
    public final void reset() {
        nanoTime = System.nanoTime();
    }

    /** Duration since creation or last reset, in seconds. */
    public double duration() {
        return (System.nanoTime() - nanoTime) / 1_000_000_000d;
    }
}
