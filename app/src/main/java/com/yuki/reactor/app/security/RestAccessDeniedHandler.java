package com.yuki.reactor.app.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yuki.reactor.api.dto.ApiError;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.MediaType;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.web.access.AccessDeniedHandler;

import java.io.IOException;

/**
 * Answers an authenticated call without the required role with 403 and the same
 * {@code {code, message}} body the REST errors use.
 */
public class RestAccessDeniedHandler implements AccessDeniedHandler {

    private static final String CODE_FORBIDDEN = "FORBIDDEN";

    private final ObjectMapper objectMapper;

    public RestAccessDeniedHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void handle(HttpServletRequest request, HttpServletResponse response,
                       AccessDeniedException accessDeniedException) throws IOException {
        response.setStatus(HttpServletResponse.SC_FORBIDDEN);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        objectMapper.writeValue(response.getWriter(),
                new ApiError(CODE_FORBIDDEN, "management endpoints require the ADMIN role"));
    }
}
