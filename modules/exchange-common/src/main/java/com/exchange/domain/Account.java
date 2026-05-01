package com.exchange.domain;

import java.util.Objects;

/**
 * Minimal account descriptor. Reused by FIX, OM, Risk, and the matching
 * engine. The numeric {@code accountId} is what travels on the wire and
 * through the event log; {@code name} is for human-facing output.
 */
public final class Account {

    private final long accountId;
    private final String name;

    public Account(long accountId, String name) {
        this.accountId = accountId;
        this.name = Objects.requireNonNull(name, "name");
    }

    public long accountId() {
        return accountId;
    }

    public String name() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Account other)) return false;
        return accountId == other.accountId && name.equals(other.name);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(accountId) * 31 + name.hashCode();
    }

    @Override
    public String toString() {
        return "Account(" + accountId + "," + name + ')';
    }
}
