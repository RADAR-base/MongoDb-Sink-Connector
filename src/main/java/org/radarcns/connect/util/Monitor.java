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

import org.slf4j.Logger;

import java.util.Collection;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Monitors a count and buffer variable by printing out their values and resetting them.
 */
public class Monitor extends TimerTask {
    private final AtomicInteger count;
    private final Logger log;
    private final String message;

    private final Collection<?> buffer;

    public Monitor(Logger log, String message) {
        this(log, message, null);
    }

    public Monitor(Logger log, String message, Collection<?> buffer) {
        if (log == null) {
            throw new IllegalArgumentException("Argument log may not be null");
        }
        this.count = new AtomicInteger(0);
        this.log = log;
        this.message = message;
        this.buffer = buffer;
    }

    /**
     * Logs the current count and, if applicable buffer size.
     *
     * <p>This resets the current count to 0.
     */
    @Override
    public void run() {
        if (buffer == null) {
            log.info("{} {}", count.getAndSet(0), message);
        } else {
            log.info("{} {} {} records need to be processed.",
                    count.getAndSet(0), message, buffer.size());
        }
    }

    /** Increment the count by one. */
    public void increment() {
        this.count.incrementAndGet();
    }

    /**
     * Add number to counter.
     */
    public void add(int number) {
        this.count.addAndGet(number);
    }

    /** Get the current count. */
    public int getCount() {
        return count.get();
    }
}
