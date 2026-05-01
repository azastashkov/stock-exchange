package com.exchange.domain;

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.util.Arrays;
import java.util.Objects;

/**
 * Registry mapping {@code String} symbol names to compact integer ids.
 * <p>
 * Symbol IDs start at 1 (0 is reserved for "unset"). The registry is loaded
 * once at startup via {@link #register(String[])} and is immutable thereafter.
 * Lookups are allocation-free in steady state.
 */
public final class Symbols {

    private final Object2IntOpenHashMap<String> nameToId;
    private final String[] idToName; // index 0 is unused

    private Symbols(Object2IntOpenHashMap<String> nameToId, String[] idToName) {
        this.nameToId = nameToId;
        this.idToName = idToName;
    }

    /**
     * Build an immutable registry from {@code names}. The element at
     * {@code names[i]} receives id {@code i + 1}. Names must be non-null,
     * non-empty, and unique.
     */
    public static Symbols register(String[] names) {
        Objects.requireNonNull(names, "names");
        Object2IntOpenHashMap<String> map = new Object2IntOpenHashMap<>(names.length * 2);
        map.defaultReturnValue(0);
        String[] idTo = new String[names.length + 1];
        for (int i = 0; i < names.length; i++) {
            String n = names[i];
            if (n == null || n.isEmpty()) {
                throw new IllegalArgumentException("Symbol name at index " + i + " is null/empty");
            }
            int id = i + 1;
            int prev = map.put(n, id);
            if (prev != 0) {
                throw new IllegalArgumentException("Duplicate symbol: " + n);
            }
            idTo[id] = n;
        }
        return new Symbols(map, idTo);
    }

    /** Returns the id for {@code name}, or throws if unknown. */
    public int idOf(String name) {
        int id = nameToId.getInt(name);
        if (id == 0) {
            throw new IllegalArgumentException("Unknown symbol: " + name);
        }
        return id;
    }

    /** Returns the name for {@code id}, or throws if unknown. */
    public String nameOf(int id) {
        if (id <= 0 || id >= idToName.length || idToName[id] == null) {
            throw new IllegalArgumentException("Unknown symbol id: " + id);
        }
        return idToName[id];
    }

    /** Allocation-free containment test. */
    public boolean contains(int id) {
        return id > 0 && id < idToName.length && idToName[id] != null;
    }

    /** Number of registered symbols. */
    public int size() {
        return idToName.length - 1;
    }

    /** Snapshot of all registered names in id order. */
    public String[] names() {
        return Arrays.copyOfRange(idToName, 1, idToName.length);
    }
}
