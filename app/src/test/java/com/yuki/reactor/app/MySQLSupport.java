package com.yuki.reactor.app;

import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * One MySQL container per test JVM, started once and never stopped explicitly: Testcontainers' Ryuk
 * removes it when the JVM exits.
 *
 * <p>Integration tests run against the same engine version as production (MySQL 8) because H2 cannot
 * prove unique constraints, conditional updates or claim races (AGENTS.md 9).
 */
public final class MySQLSupport {

    private static final MySQLContainer<?> CONTAINER =
            new MySQLContainer<>(DockerImageName.parse("mysql:8.4"));

    static {
        CONTAINER.start();
    }

    private MySQLSupport() {
    }

    public static String jdbcUrl() {
        return "jdbc:mysql://" + CONTAINER.getHost() + ":" + CONTAINER.getMappedPort(3306)
                + "/" + CONTAINER.getDatabaseName()
                + "?connectionTimeZone=UTC&characterEncoding=UTF-8";
    }

    public static String username() {
        return CONTAINER.getUsername();
    }

    public static String password() {
        return CONTAINER.getPassword();
    }
}
