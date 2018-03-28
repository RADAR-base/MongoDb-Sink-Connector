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

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAmount;

/**
 * Measure the duration of an action. Not thread-safe.
 */
public class OperationTimer {
    private final Logger logger;
    private final String operation;
    private Instant startTime;

    public OperationTimer(Logger logger, String operation) {
        this.logger = logger;
        this.operation = operation;
    }

    public void start() {
        if (!logger.isDebugEnabled()) {
            return;
        }
        startTime = Instant.now();
        logger.debug("Start {}", operation);
    }

    /** Duration since creation or last reset, in seconds. */
    public TemporalAmount duration() {
        return Duration.between(startTime, Instant.now());
    }

    public void log() {
        if (!logger.isDebugEnabled()) {
            return;
        }
        logger.debug("[{}] Time elapsed: {}", operation, duration());
    }

    public void stop() {
        log();
        logger.debug("End {}", operation);
    }
}
