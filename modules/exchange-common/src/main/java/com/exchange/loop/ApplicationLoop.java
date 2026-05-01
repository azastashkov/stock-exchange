package com.exchange.loop;

import com.exchange.affinity.CpuAffinity;
import com.exchange.affinity.CpuAffinity.AffinityHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.LockSupport;

/**
 * Single-threaded busy-poll loop. Each loop owns one thread, optionally
 * pinned to a dedicated core. Subclasses implement {@link #pollOnce()}
 * which returns true if it did any useful work; the loop adaptively
 * parks the thread for 1ns after ~1024 idle iterations to keep the core
 * from spinning at 100% when the system is quiet.
 */
public abstract class ApplicationLoop {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationLoop.class);

    /** Iterations of unproductive polling before we start parking. */
    static final int IDLE_PARK_THRESHOLD = 1024;

    private final String name;
    private final boolean cpuPinning;
    private Thread thread;
    private volatile boolean running = false;
    private volatile int idleCounter = 0;

    protected ApplicationLoop(String name, boolean cpuPinning) {
        this.name = name;
        this.cpuPinning = cpuPinning;
    }

    /**
     * Run one iteration of useful work. Return true if any work was
     * performed; false if there was nothing to do. The loop uses this
     * signal to back off and park briefly on idle.
     */
    protected abstract boolean pollOnce();

    /** Hook invoked once before the polling loop starts. Default no-op. */
    protected void onStart() {
        // override
    }

    /** Hook invoked once after the polling loop exits. Default no-op. */
    protected void onStop() {
        // override
    }

    public final synchronized void start() {
        if (thread != null) {
            throw new IllegalStateException("Loop '" + name + "' already started");
        }
        running = true;
        thread = new Thread(this::run, name);
        thread.setDaemon(true);
        thread.start();
    }

    public final void stop() {
        running = false;
        Thread t;
        synchronized (this) {
            t = thread;
        }
        if (t == null) return;
        try {
            t.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public final String name() {
        return name;
    }

    public final boolean isRunning() {
        return running;
    }

    /** Test hook: number of consecutive idle polls observed. */
    public final int idleCounter() {
        return idleCounter;
    }

    private void run() {
        AffinityHandle handle = cpuPinning ? CpuAffinity.acquireCore(name) : AffinityHandle.NOOP;
        try {
            onStart();
            int idle = 0;
            while (running) {
                boolean did;
                try {
                    did = pollOnce();
                } catch (Throwable t) {
                    // never silently swallow; continue running so the loop survives
                    // a bad event but the failure is visible in logs.
                    LOG.error("Uncaught error in pollOnce of loop '{}'", name, t);
                    did = false;
                }
                if (did) {
                    idle = 0;
                } else if (++idle > IDLE_PARK_THRESHOLD) {
                    LockSupport.parkNanos(1L);
                    idle = IDLE_PARK_THRESHOLD;
                }
                idleCounter = idle;
            }
        } finally {
            try {
                onStop();
            } catch (Throwable t) {
                LOG.error("Error in onStop hook of loop '{}'", name, t);
            }
            handle.close();
        }
    }
}
