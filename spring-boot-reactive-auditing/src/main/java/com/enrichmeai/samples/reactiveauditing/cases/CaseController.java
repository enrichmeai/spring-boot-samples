package com.enrichmeai.samples.reactiveauditing.cases;

import java.util.NoSuchElementException;
import java.util.UUID;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/cases")
class CaseController {

    record CaseRequest(String title) {}

    private final CaseService service;

    CaseController(CaseService service) {
        this.service = service;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    Mono<Case> create(@RequestBody CaseRequest request) {
        return service.create(request.title());
    }

    @PostMapping("/{id}/rename")
    Mono<Case> rename(@PathVariable UUID id, @RequestBody CaseRequest request) {
        return service.rename(id, request.title());
    }

    @PostMapping("/{id}/close")
    Mono<Case> close(@PathVariable UUID id) {
        return service.close(id);
    }

    @ExceptionHandler(NoSuchElementException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    void notFound() {
        // 404, no body
    }
}
