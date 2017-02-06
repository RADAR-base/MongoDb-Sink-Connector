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
