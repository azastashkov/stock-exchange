package com.exchange.loadclient;

import com.exchange.time.Clocks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Top-level orchestrator. Owns N {@link SessionRunner}s, a single
 * {@link RateLimiter}, and a scheduler thread that fans tokens out to
 * sessions round-robin so the aggregate send rate matches
 * {@link LoadClientConfig#rate()}.
 */
public final class LoadClient {

    private static final Logger LOG = LoggerFactory.getLogger(LoadClient.class);

    private final LoadClientConfig cfg;
    private final LatencyRecorder latency;
    private final LeaderTracker tracker;
    private final List<SessionRunner> sessions;
    private final AtomicBoolean stop = new AtomicBoolean(false);
    private Thread scheduler;

    public LoadClient(LoadClientConfig cfg) {
        this.cfg = cfg;
        this.latency = new LatencyRecorder();
        this.tracker = new LeaderTracker(cfg.targets());
        this.sessions = new ArrayList<>(cfg.sessions());
        for (int i = 0; i < cfg.sessions(); i++) {
            sessions.add(new SessionRunner(i, cfg, tracker, latency, stop));
        }
    }

    /**
     * Run the load test for {@link LoadClientConfig#durationSec()} seconds.
     * Returns the exit code: 0 if {@code count > 0 && p99 < ceiling}, else 1.
     */
    public int run() {
        LOG.info("Starting load client: {}", cfg);

        // Start sessions; each one connects + logons asynchronously in its own thread.
        for (SessionRunner sr : sessions) sr.start();

        // Wait for at least one session to be ready (best-effort).
        sleep(500L);

        // Scheduler thread: drives rate by minting tokens.
        long startNs = Clocks.nanoTime();
        long durationNs = (long) cfg.durationSec() * 1_000_000_000L;
        long warmEndsNs = startNs + (long) cfg.warmUpSec() * 1_000_000_000L;
        RateLimiter limiter = new RateLimiter(cfg.rate(), startNs);
        scheduler = new Thread(() -> {
            int rr = 0;
            while (!stop.get()) {
                long now = Clocks.nanoTime();
                if (now - startNs >= durationNs) {
                    stop.set(true);
                    break;
                }
                limiter.bulkRefill(now);
                int dispensed = 0;
                while (limiter.tryAcquire() && dispensed < 1024) {
                    SessionRunner sr = sessions.get(rr);
                    rr = (rr + 1) % sessions.size();
                    sr.offerToken();
                    dispensed++;
                }
                if (dispensed == 0) {
                    // Park briefly so we don't burn cpu when the bucket is empty.
                    sleep(1L);
                } else {
                    // small yield
                    Thread.yield();
                }
            }
        }, "load-scheduler");
        scheduler.setDaemon(true);
        scheduler.start();

        // Periodic progress
        long lastReport = startNs;
        while (!stop.get()) {
            sleep(1000L);
            long now = Clocks.nanoTime();
            if (now - startNs >= durationNs) {
                stop.set(true);
                break;
            }
            if (now - lastReport >= 5_000_000_000L) {
                long sent = totalSent();
                long cancels = totalCancels();
                long total = latency.totalCount();
                long elapsedSec = (now - startNs) / 1_000_000_000L;
                if (now < warmEndsNs) {
                    LOG.info("[warmup t={}s] sent {} new, {} cancels, {} responses",
                            elapsedSec, sent, cancels, total);
                } else {
                    LOG.info("[t={}s] sent {} new, {} cancels, {} responses (p99 so far: {} ms)",
                            elapsedSec, sent, cancels, total,
                            String.format("%.2f", latency.p99Ms()));
                }
                lastReport = now;
            }
        }
        LOG.info("Stop requested; draining tail responses...");

        // Wait for sessions to drain & logout.
        long shutdownDeadline = System.currentTimeMillis() + 5000L;
        for (SessionRunner sr : sessions) {
            long remaining = Math.max(50L, shutdownDeadline - System.currentTimeMillis());
            sr.awaitTermination(remaining);
        }
        try { scheduler.join(2000L); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }

        long endNs = Clocks.nanoTime();
        long durationMs = (endNs - startNs) / 1_000_000L;
        long sent = totalSent();
        long cancels = totalCancels();
        LOG.info("Final: sent {} new, {} cancels", sent, cancels);

        boolean pass = latency.printStatsAndCheck(cfg.p99CeilingMs(), cfg.sessions(), durationMs);
        return pass ? 0 : 1;
    }

    /** External hook (for tests / signals): request graceful shutdown. */
    public void requestStop() {
        stop.set(true);
    }

    private long totalSent() {
        long s = 0L;
        for (SessionRunner sr : sessions) s += sr.ordersSent();
        return s;
    }

    private long totalCancels() {
        long s = 0L;
        for (SessionRunner sr : sessions) s += sr.cancelsSent();
        return s;
    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    }
}
