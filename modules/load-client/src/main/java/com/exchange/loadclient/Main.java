package com.exchange.loadclient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CLI entry point for the load client. Supports the small set of flags
 * documented in {@link #USAGE}; uses a hand-rolled parser to avoid pulling
 * in a CLI library.
 */
public final class Main {

    public static final String USAGE =
            "Usage: load-client [options]\n"
                    + "  Required:\n"
                    + "    --targets=host1:port,host2:port,...   Comma-separated target gateways\n"
                    + "    --rate=N                              Aggregate orders per second\n"
                    + "    --duration=N                          Duration in seconds\n"
                    + "    --symbols=AAPL,MSFT,...               Comma-separated symbols\n"
                    + "  Optional:\n"
                    + "    --sessions=N                          Number of FIX sessions (default 20)\n"
                    + "    --p99-ceiling-ms=N                    Pass/fail p99 ceiling in ms (default 5)\n"
                    + "    --account=N                           Account id (default 1)\n"
                    + "    --price-base=D                        Per-symbol mid price (default 100.00)\n"
                    + "    --warm-up-secs=N                      Warm-up window in seconds (default 5)\n"
                    + "    --help                                Show this message\n";

    public static void main(String[] args) {
        int code = run(args);
        System.exit(code);
    }

    /** Parse and execute. Returns the desired exit code. */
    public static int run(String[] args) {
        for (String a : args) {
            if ("--help".equals(a) || "-h".equals(a)) {
                System.out.println(USAGE);
                return 0;
            }
        }
        LoadClientConfig cfg;
        try {
            cfg = parse(args);
        } catch (IllegalArgumentException iae) {
            System.err.println("error: " + iae.getMessage());
            System.err.println();
            System.err.println(USAGE);
            return 2;
        }
        LoadClient client = new LoadClient(cfg);
        Runtime.getRuntime().addShutdownHook(new Thread(client::requestStop, "load-client-shutdown"));
        try {
            return client.run();
        } catch (RuntimeException re) {
            System.err.println("load-client crashed: " + re);
            re.printStackTrace(System.err);
            return 3;
        }
    }

    /** Parse a {@code key=value} flags array into a {@link LoadClientConfig}. */
    public static LoadClientConfig parse(String[] args) {
        Map<String, String> kv = new HashMap<>();
        List<String> bare = new ArrayList<>();
        for (String a : args) {
            if (a == null || a.isEmpty()) continue;
            if (!a.startsWith("--")) {
                bare.add(a);
                continue;
            }
            String body = a.substring(2);
            int eq = body.indexOf('=');
            if (eq < 0) {
                kv.put(body, "true");
            } else {
                kv.put(body.substring(0, eq), body.substring(eq + 1));
            }
        }
        if (!bare.isEmpty()) {
            throw new IllegalArgumentException("unexpected positional args: " + bare);
        }

        // Required
        String tgts = required(kv, "targets");
        String rate = required(kv, "rate");
        String dur = required(kv, "duration");
        String syms = required(kv, "symbols");

        List<String> targets = parseList(tgts);
        if (targets.isEmpty()) {
            throw new IllegalArgumentException("--targets must be a non-empty comma-separated list");
        }
        for (String t : targets) {
            int colon = t.lastIndexOf(':');
            if (colon < 0 || colon == t.length() - 1) {
                throw new IllegalArgumentException("--targets entry must be host:port: " + t);
            }
            try {
                int port = Integer.parseInt(t.substring(colon + 1));
                if (port <= 0 || port > 65535) {
                    throw new IllegalArgumentException("--targets port out of range: " + t);
                }
            } catch (NumberFormatException nfe) {
                throw new IllegalArgumentException("--targets port not numeric: " + t);
            }
        }

        int rateInt = parseInt("rate", rate);
        if (rateInt <= 0) throw new IllegalArgumentException("--rate must be > 0");
        int durSec = parseInt("duration", dur);
        if (durSec <= 0) throw new IllegalArgumentException("--duration must be > 0");

        List<String> symbols = parseList(syms);
        if (symbols.isEmpty()) {
            throw new IllegalArgumentException("--symbols must be a non-empty comma-separated list");
        }

        // Optional
        int sessions = parseInt("sessions", kv.getOrDefault("sessions", "20"));
        if (sessions <= 0) throw new IllegalArgumentException("--sessions must be > 0");
        double p99Ceiling = parseDouble("p99-ceiling-ms", kv.getOrDefault("p99-ceiling-ms", "5"));
        if (p99Ceiling <= 0) throw new IllegalArgumentException("--p99-ceiling-ms must be > 0");
        long account = parseLong("account", kv.getOrDefault("account", "1"));
        long priceBaseFp = parsePriceFp(kv.getOrDefault("price-base", "100.00"));
        int warmUpSec = parseInt("warm-up-secs", kv.getOrDefault("warm-up-secs", "5"));
        if (warmUpSec < 0) throw new IllegalArgumentException("--warm-up-secs must be >= 0");

        return new LoadClientConfig(targets, rateInt, durSec, symbols,
                sessions, p99Ceiling, account, priceBaseFp, warmUpSec);
    }

    private static String required(Map<String, String> kv, String name) {
        String v = kv.get(name);
        if (v == null || v.isEmpty()) {
            throw new IllegalArgumentException("missing required flag --" + name);
        }
        return v;
    }

    private static int parseInt(String name, String v) {
        try {
            return Integer.parseInt(v);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("--" + name + " not an integer: " + v);
        }
    }

    private static long parseLong(String name, String v) {
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("--" + name + " not a long: " + v);
        }
    }

    private static double parseDouble(String name, String v) {
        try {
            return Double.parseDouble(v);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("--" + name + " not a number: " + v);
        }
    }

    /** Parse a decimal price as fixed-point times 10000. Accepts "100", "100.5", "100.0001". */
    static long parsePriceFp(String s) {
        if (s == null || s.isEmpty()) {
            throw new IllegalArgumentException("--price-base must not be empty");
        }
        boolean neg = false;
        int i = 0;
        if (s.charAt(0) == '-') { neg = true; i = 1; }
        long whole = 0L, frac = 0L;
        int fracDigits = 0;
        boolean dot = false;
        for (; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '.') {
                if (dot) throw new IllegalArgumentException("--price-base has two decimals: " + s);
                dot = true;
                continue;
            }
            if (c < '0' || c > '9') {
                throw new IllegalArgumentException("--price-base not numeric: " + s);
            }
            if (!dot) {
                whole = whole * 10L + (c - '0');
            } else {
                if (fracDigits >= 4) {
                    // truncate beyond 4 decimal digits
                    continue;
                }
                frac = frac * 10L + (c - '0');
                fracDigits++;
            }
        }
        while (fracDigits < 4) {
            frac *= 10L;
            fracDigits++;
        }
        long fp = whole * 10000L + frac;
        return neg ? -fp : fp;
    }

    private static List<String> parseList(String s) {
        List<String> out = new ArrayList<>();
        for (String t : s.split(",")) {
            String trimmed = t.trim();
            if (!trimmed.isEmpty()) out.add(trimmed);
        }
        return out;
    }

    /** Visible for tests: helper to format a parsed config for user inspection. */
    static String describe(LoadClientConfig cfg) {
        return cfg.toString() + " targets=" + Arrays.toString(cfg.targets().toArray());
    }
}
