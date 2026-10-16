package com.yuki.reactor.app.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yuki.reactor.api.dto.ApiError;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.MediaType;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;

import java.io.IOException;

/**
 * Answers an unauthenticated call to a management endpoint with 401 and the same
 * {@code {code, message}} body the REST errors use, instead of a browser login page.
 */
public class RestAuthenticationEntryPoint implements AuthenticationEntryPoint {

    private static final String CODE_UNAUTHORIZED = "UNAUTHORIZED";

    private final ObjectMapper objectMapper;

    public RestAuthenticationEntryPoint(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void commence(HttpServletRequest request, HttpServletResponse response,
                         AuthenticationException authException) throws IOException {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setHeader("WWW-Authenticate", "Basic realm=\"reactor\"");
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        objectMapper.writeValue(response.getWriter(),
                new ApiError(CODE_UNAUTHORIZED, "management endpoints require authentication"));
    }
}
