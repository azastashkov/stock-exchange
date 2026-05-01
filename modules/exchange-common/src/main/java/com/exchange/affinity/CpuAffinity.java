package com.exchange.affinity;

import net.openhft.affinity.AffinityLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Pin the calling thread to a dedicated physical core via OpenHFT's
 * {@link AffinityLock}. On non-Linux platforms (or when the affinity
 * library cannot acquire a core) we degrade gracefully to a no-op handle
 * and warn once.
 */
public final class CpuAffinity {

    private static final Logger LOG = LoggerFactory.getLogger(CpuAffinity.class);
    private static final AtomicBoolean WARNED = new AtomicBoolean(false);

    private CpuAffinity() {
        // utility class
    }

    /**
     * Acquire a dedicated core for the current thread. The returned handle
     * MUST be closed (try-with-resources) to release the core back to the
     * pool. On non-Linux platforms a no-op handle is returned.
     */
    public static AffinityHandle acquireCore(String name) {
        if (!isLinux()) {
            warnOnce(name);
            return AffinityHandle.NOOP;
        }
        try {
            AffinityLock lock = AffinityLock.acquireCore();
            return new RealHandle(lock, name);
        } catch (Throwable t) {
            warnOnce(name);
            LOG.warn("Failed to acquire core for thread '{}': {}", name, t.toString());
            return AffinityHandle.NOOP;
        }
    }

    private static void warnOnce(String name) {
        if (WARNED.compareAndSet(false, true)) {
            LOG.warn("CPU affinity not available on this platform ({}); thread '{}' and others will run unpinned",
                    System.getProperty("os.name"), name);
        }
    }

    private static boolean isLinux() {
        String os = System.getProperty("os.name");
        return os != null && os.toLowerCase().contains("linux");
    }

    /**
     * AutoCloseable handle representing the lifetime of a core reservation.
     */
    public interface AffinityHandle extends AutoCloseable {
        AffinityHandle NOOP = () -> { /* no-op */ };

        @Override
        void close();
    }

    private static final class RealHandle implements AffinityHandle {
        private final AffinityLock lock;
        private final String name;

        RealHandle(AffinityLock lock, String name) {
            this.lock = lock;
            this.name = name;
        }

        @Override
        public void close() {
            try {
                lock.release();
            } catch (Throwable t) {
                LOG.warn("Failed to release core for thread '{}': {}", name, t.toString());
            }
        }
    }
}
