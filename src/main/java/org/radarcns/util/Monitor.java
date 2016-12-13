package org.radarcns.util;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

public class Monitor extends TimerTask {
    private final AtomicInteger count;
    private final Logger log;
    private final String message;

    private final Collection<?> buffer;

    public Monitor(@Nonnull Logger log, @Nonnull AtomicInteger count, String message) {
        this(log, count, message, null);
    }

    public Monitor(@Nonnull Logger log, @Nonnull AtomicInteger count, String message,
                   Collection<?> buffer) {
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
