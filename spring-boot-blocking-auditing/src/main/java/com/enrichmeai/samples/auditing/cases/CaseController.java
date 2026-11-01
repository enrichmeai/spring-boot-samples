package com.enrichmeai.samples.auditing.cases;

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

@RestController
@RequestMapping("/cases")
class CaseController {

    record CaseRequest(String title) {}

    record CaseResponse(UUID id, String title, String status) {
        static CaseResponse of(Case c) {
            return new CaseResponse(c.getId(), c.getTitle(), c.getStatus());
        }
    }

    private final CaseService service;

    CaseController(CaseService service) {
        this.service = service;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    CaseResponse create(@RequestBody CaseRequest request) {
        return CaseResponse.of(service.create(request.title()));
    }

    @PostMapping("/{id}/rename")
    CaseResponse rename(@PathVariable UUID id, @RequestBody CaseRequest request) {
        return CaseResponse.of(service.rename(id, request.title()));
    }

    @PostMapping("/{id}/close")
    CaseResponse close(@PathVariable UUID id) {
        return CaseResponse.of(service.close(id));
    }

    @ExceptionHandler(NoSuchElementException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    void notFound() {
        // 404, no body
    }
}
