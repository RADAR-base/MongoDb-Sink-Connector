package org.radarcns.util;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Francesco Nobilia on 28/11/2016.
 */
public class Monitor extends TimerTask {
    private final AtomicInteger count;
    private final Logger log;
    private final String message;

    private Collection<?> buffer;

    public Monitor(AtomicInteger count, String message, Logger log) {
        this.count = count;
        this.message = message;
        this.log = log;
    }

    public Monitor(AtomicInteger count, Logger log, String message, Collection<?> buffer) {
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
            log.info("{} {} {} records need to be processed.", count.getAndSet(0), message, buffer.size());
        }
    }
}
