package com.itsaur.repository;

import com.itsaur.model.Company;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface CompanyRepository {
    Mono<Company> getCompanyById(Long id);
    Mono<Boolean> existsVat(String vat);
}
