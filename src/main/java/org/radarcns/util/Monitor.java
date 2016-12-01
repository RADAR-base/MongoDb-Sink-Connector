package org.radarcns.util;

import org.slf4j.Logger;

import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Francesco Nobilia on 30/11/2016.
 */
public class Monitor extends TimerTask {

    private final AtomicInteger count;
    private final Logger log;
    private final String message;

    public Monitor(AtomicInteger count, String message, Logger log) {
        this.count = count;
        this.message = message;
        this.log = log;
    }

    @Override
    public void run() {
        log.info("{} {}",count.getAndSet(0),message);
    }
}
