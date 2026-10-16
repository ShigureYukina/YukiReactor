package com.yuki.reactor.api.dto;

/**
 * Uniform error body for every failed API call: {@code {"code": "...", "message": "..."}}.
 */
public record ApiError(String code, String message) {
}
