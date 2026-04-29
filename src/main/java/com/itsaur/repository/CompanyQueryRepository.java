package com.itsaur.repository;

import com.itsaur.repository.search.SearchComponent;
import com.itsaur.model.CompanyQueryModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface CompanyQueryRepository {
    Mono<CompanyQueryModel> getCompanyById(Long id);
    Flux<CompanyQueryModel> getAllCompanies();
    Flux<CompanyQueryModel> findPage(Integer page, Integer size);
    Mono<Long> countAll();
    Flux<CompanyQueryModel> paginatedCompanyFilterQuery(SearchComponent searchComponent, Integer page, Integer size);
    Mono<Long> countViewCompanies(SearchComponent searchComponent);
}
