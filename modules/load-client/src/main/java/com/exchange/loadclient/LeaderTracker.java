package com.exchange.loadclient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tiny shared state map: tracks which targets are currently believed to be
 * leaders, and (when the cluster includes node-id hints) maps reported node
 * ids back to compose hostnames.
 * <p>
 * Used by {@link SessionRunner} when picking a next-target after a
 * "NOT LEADER" reject. The tracker is best-effort: redirects are advisory,
 * the next reject will correct any misroute.
 */
public final class LeaderTracker {

    private static final Pattern NODE_HINT = Pattern.compile("use node\\s+(\\d+)");

    private final List<String> targets;
    /** target → suspected to be leader (true) or known-not-leader (false). */
    private final ConcurrentHashMap<String, Boolean> leaderHint = new ConcurrentHashMap<>();
    /** Last hinted leader, used to fan-out to a single target after a redirect. */
    private final AtomicReference<String> lastKnownLeader = new AtomicReference<>();

    public LeaderTracker(List<String> targets) {
        this.targets = List.copyOf(targets);
        for (String t : targets) {
            leaderHint.put(t, Boolean.TRUE); // optimistic: anyone could be leader
        }
    }

    /**
     * Parse the {@code 58=Text} contents of a NOT LEADER ExecutionReport and
     * (if a numeric "use node N" hint is present, and N maps to one of our
     * targets) flag that target as the leader.
     */
    public void onNotLeader(String currentTarget, String text) {
        if (currentTarget != null) {
            leaderHint.put(currentTarget, Boolean.FALSE);
        }
        if (text == null) return;
        Matcher m = NODE_HINT.matcher(text);
        if (!m.find()) return;
        int nodeId;
        try {
            nodeId = Integer.parseInt(m.group(1));
        } catch (NumberFormatException ignored) {
            return;
        }
        // Compose convention: target hostnames are like "exchange-N:port". If we
        // can find a target whose host substring contains "-N", flag it.
        String suffix = "-" + nodeId;
        String suffixColon = "-" + nodeId + ":";
        for (String t : targets) {
            int colon = t.indexOf(':');
            String host = colon >= 0 ? t.substring(0, colon) : t;
            if (host.endsWith(suffix) || t.contains(suffixColon)) {
                leaderHint.put(t, Boolean.TRUE);
                lastKnownLeader.set(t);
                return;
            }
        }
    }

    /** Mark a target as known-leader after it accepted an order. */
    public void onLeaderAccepted(String target) {
        leaderHint.put(target, Boolean.TRUE);
        lastKnownLeader.set(target);
    }

    /**
     * Pick a next target after {@code current} was rejected. Prefers the
     * last-known leader; otherwise round-robins past {@code current}.
     */
    public String nextTarget(String current) {
        String hint = lastKnownLeader.get();
        if (hint != null && !hint.equals(current) && leaderHint.getOrDefault(hint, Boolean.TRUE)) {
            return hint;
        }
        // Round-robin past current. If unknown, just start at index 0.
        int idx = -1;
        for (int i = 0; i < targets.size(); i++) {
            if (targets.get(i).equals(current)) {
                idx = i;
                break;
            }
        }
        if (idx < 0) return targets.get(0);
        // Try targets in cyclic order; prefer not-known-bad first.
        int n = targets.size();
        // First pass: skip targets we've marked false unless they all are.
        List<String> candidates = new ArrayList<>(n);
        for (int i = 1; i <= n; i++) {
            String t = targets.get((idx + i) % n);
            if (leaderHint.getOrDefault(t, Boolean.TRUE)) candidates.add(t);
        }
        if (!candidates.isEmpty()) return candidates.get(0);
        // Otherwise cycle anyway.
        return targets.get((idx + 1) % n);
    }

    /** Snapshot of the current per-target hint (for tests). */
    public boolean isHintedLeader(String target) {
        return leaderHint.getOrDefault(target, Boolean.FALSE);
    }
}
