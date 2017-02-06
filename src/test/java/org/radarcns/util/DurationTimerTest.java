package org.radarcns.util;

import org.junit.Test;

import static org.junit.Assert.*;

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