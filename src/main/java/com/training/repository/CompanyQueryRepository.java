package com.training.repository;

import com.training.repository.search.SearchComponent;
import com.training.model.CompanyQueryModel;
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
