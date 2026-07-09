package com.training.service;

import com.training.dto.PageDTO;
import com.training.repository.search.SearchComponent;
import com.training.model.CompanyQueryModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface CompanyQueryService {
    Flux<CompanyQueryModel> findAllCompanies();
    Mono<CompanyQueryModel> findCompanyById(Long id);
    Mono<PageDTO<CompanyQueryModel>> getPage(int page, int size);
    Mono<PageDTO<CompanyQueryModel>> getPaginatedSearchCompanies(SearchComponent searchComponent, int page, int size);
}
