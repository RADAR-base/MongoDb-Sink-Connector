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

import org.slf4j.Logger;

import java.util.Collection;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

public class Monitor extends TimerTask {
    private final AtomicInteger count;
    private final Logger log;
    private final String message;

    private final Collection<?> buffer;

    public Monitor(Logger log, AtomicInteger count, String message) {
        this(log, count, message, null);
    }

    public Monitor(Logger log, AtomicInteger count, String message,
                   Collection<?> buffer) {
        if (log == null || count == null) {
            throw new NullPointerException("Parameters log and count may not be null");
        }
        this.count = count;
        this.log = log;
        this.message = message;
        this.buffer = buffer;
    }

    @Override
    public void run() {
        if(buffer == null) {
            log.info("{} {}", count.getAndSet(0), message);
        }
        else {
            log.info("{} {} {} records need to be processed.", count.getAndSet(0), message,
                    buffer.size());
        }
    }
}
