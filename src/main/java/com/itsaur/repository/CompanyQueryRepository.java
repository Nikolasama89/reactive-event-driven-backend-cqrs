package com.itsaur.repository;

import com.itsaur.model.Company;
import com.itsaur.model.CompanyQueryModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface CompanyQueryRepository {
    Mono<CompanyQueryModel> getCompanyById(Long id);
    Flux<CompanyQueryModel> getAllCompanies();
    Flux<CompanyQueryModel> findPage(Integer page, Integer size);
    Mono<Long> countAll();
}
