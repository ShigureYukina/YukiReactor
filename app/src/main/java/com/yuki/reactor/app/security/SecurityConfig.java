package com.yuki.reactor.app.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;

import java.util.ArrayList;
import java.util.List;

/**
 * Authentication and authorization for the management endpoints (PRD 8.4, phase 5).
 *
 * <p>Cancel, replay and fault injection are operational writes: they require a configured user
 * with {@code ROLE_ADMIN} via HTTP Basic. Everything else (task submission, queries, executions,
 * timeline, decisions, metrics, experiment records, the static panel and the actuator read
 * endpoints) stays open - PRD 8.4 only mandates protecting the management interfaces.
 *
 * <p>Rejections use the same {@code {code, message}} JSON shape as the REST error responses, so
 * clients never receive an HTML login page. Passwords come from configuration and are encoded in
 * memory at startup; the development default must be overridden outside a local machine.
 */
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    private final SecurityProperties properties;
    private final ObjectMapper objectMapper;

    public SecurityConfig(SecurityProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        // One entry point for both rejection paths: a wrong password fails inside the basic auth
        // filter (httpBasic), a missing one fails later in the exception handling filter.
        RestAuthenticationEntryPoint entryPoint = new RestAuthenticationEntryPoint(objectMapper);
        http
                .csrf(csrf -> csrf.disable())
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .httpBasic(httpBasic -> httpBasic.authenticationEntryPoint(entryPoint))
                .exceptionHandling(handling -> handling
                        .authenticationEntryPoint(entryPoint)
                        .accessDeniedHandler(new RestAccessDeniedHandler(objectMapper)))
                .authorizeHttpRequests(requests -> requests
                        .requestMatchers(HttpMethod.POST, "/api/v1/tasks/*/cancel").hasRole("ADMIN")
                        .requestMatchers(HttpMethod.POST, "/api/v1/tasks/*/replay").hasRole("ADMIN")
                        .requestMatchers(HttpMethod.PUT, "/api/v1/fault-lab/downstream").hasRole("ADMIN")
                        .anyRequest().permitAll());
        return http.build();
    }

    @Bean
    public UserDetailsService userDetailsService(PasswordEncoder encoder) {
        List<UserDetails> users = new ArrayList<>();
        properties.getUsers().forEach((username, user) -> {
            if (user.password() == null || user.password().isBlank()) {
                throw new IllegalStateException(
                        "reactor.security.users." + username + ".password must not be blank");
            }
            users.add(User.withUsername(username)
                    .password(encoder.encode(user.password()))
                    .roles(user.roles().toArray(String[]::new))
                    .build());
        });
        return new InMemoryUserDetailsManager(users);
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return PasswordEncoderFactories.createDelegatingPasswordEncoder();
    }
}
