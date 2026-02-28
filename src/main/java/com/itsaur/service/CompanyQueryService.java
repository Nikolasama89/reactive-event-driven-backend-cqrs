package com.itsaur.service;

import com.itsaur.dto.PageDTO;
import com.itsaur.dto.SearchComponent;
import com.itsaur.model.CompanyQueryModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface CompanyQueryService {
    Flux<CompanyQueryModel> findAllCompanies();
    Mono<CompanyQueryModel> findCompanyById(Long id);
    Mono<PageDTO<CompanyQueryModel>> getPage(int page, int size);
    Mono<PageDTO<CompanyQueryModel>> getPaginatedSearchCompanies(SearchComponent searchComponent, int page, int size);
}
