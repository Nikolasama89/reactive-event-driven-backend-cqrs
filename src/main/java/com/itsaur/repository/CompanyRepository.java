package com.itsaur.repository;

import com.itsaur.model.Company;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface CompanyRepository {
    Mono<Company> getCompanyById(Long id);
    Mono<Company> createOrUpdateCompany(Long id, Company company);
    Mono<Void> deleteCompany(Long id);
    Mono<Boolean> existsVat(String vat);
}
