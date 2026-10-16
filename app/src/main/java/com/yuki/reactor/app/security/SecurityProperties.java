package com.yuki.reactor.app.security;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Users allowed to call the management endpoints (PRD 8.4, phase 5): task cancel, task replay and
 * fault injection. Read endpoints stay open, so the panel, the experiment scripts and monitoring
 * keep working without credentials.
 *
 * <p>Configuration shape:
 * <pre>
 * reactor.security.users.admin.password=${REACTOR_ADMIN_PASSWORD:reactor-dev}
 * reactor.security.users.admin.roles=ADMIN
 * </pre>
 */
@ConfigurationProperties(prefix = "reactor.security")
public class SecurityProperties {

    /** Credentials keyed by username; a blank password fails fast at startup. */
    private Map<String, User> users = new LinkedHashMap<>();

    public Map<String, User> getUsers() {
        return users;
    }

    public void setUsers(Map<String, User> users) {
        this.users = users;
    }

    /** One configured user: a password and the roles granted to it. */
    public record User(String password, List<String> roles) {

        public User {
            roles = roles == null ? List.of() : List.copyOf(roles);
        }
    }
}
