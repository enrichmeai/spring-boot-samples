package com.enrichmeai.samples.reactiveauditing;

import java.time.Duration;
import java.util.List;

import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.ReactiveSecurityContextHolder;
import org.springframework.test.context.DynamicPropertyRegistry;

import reactor.core.publisher.Mono;
import reactor.util.context.Context;

final class IntegrationTestSupport {

    static final Duration TIMEOUT = Duration.ofSeconds(30);

    private IntegrationTestSupport() {
    }

    static void postgres(DynamicPropertyRegistry registry) {
        var pg = Containers.postgres();
        registry.add("spring.r2dbc.url", () -> "r2dbc:postgresql://" + pg.getHost() + ":"
                + pg.getMappedPort(5432) + "/" + pg.getDatabaseName());
        registry.add("spring.r2dbc.username", pg::getUsername);
        registry.add("spring.r2dbc.password", pg::getPassword);
        registry.add("spring.flyway.url", pg::getJdbcUrl);
        registry.add("spring.flyway.user", pg::getUsername);
        registry.add("spring.flyway.password", pg::getPassword);
    }

    /** Runs {@code mono} as the given user, as WebFlux security would for a request. */
    static <T> Mono<T> as(String user, Mono<T> mono) {
        Context ctx = ReactiveSecurityContextHolder.withAuthentication(
                UsernamePasswordAuthenticationToken.authenticated(user, null, List.of()));
        return mono.contextWrite(ctx);
    }
}
