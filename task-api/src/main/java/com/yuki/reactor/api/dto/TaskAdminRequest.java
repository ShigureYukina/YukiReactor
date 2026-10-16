package com.yuki.reactor.api.dto;

/**
 * Body of the administrative endpoints (cancel, replay).
 *
 * <p>Both fields are mandatory: an administrative write without a recorded operator and reason is
 * not auditable, so the API refuses it (AGENTS.md 8).
 */
public record TaskAdminRequest(String operator, String reason) {
}
