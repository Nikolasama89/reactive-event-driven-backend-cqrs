package com.itsaur.service;

import com.itsaur.dto.PageDTO;
import com.itsaur.exceptions.CompanyNotFoundException;
import com.itsaur.exceptions.EntityInvalidArgumentException;
import com.itsaur.model.CompanyQueryModel;
import com.itsaur.repository.CompanyQueryRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class CompanyQueryServiceImpl implements CompanyQueryService{

    private final CompanyQueryRepository companyQueryRepository;

    public CompanyQueryServiceImpl(CompanyQueryRepository companyQueryRepository){
        this.companyQueryRepository = companyQueryRepository;
    }

    @Override
    public Flux<CompanyQueryModel> findAllCompanies() {
        return companyQueryRepository.getAllCompanies();
    }

    @Override
    public Mono<CompanyQueryModel> findCompanyById(Long id) {
        if (id == 0 || id < 0) {
            return Mono.error(new EntityInvalidArgumentException("Id can not be null or empty"));
        }
        return companyQueryRepository.getCompanyById(id)
                .switchIfEmpty(Mono.error(new CompanyNotFoundException("Company with id: " + id + " not found")));
    }

    @Override
    public Mono<PageDTO<CompanyQueryModel>> getPage(int page, int size) {
        if (page < 0 || size <= 0) {
            return Mono.error(new EntityInvalidArgumentException("Page and size cannot be null"));
        }

        return Mono.zip(
                companyQueryRepository.findPage(page, size).collectList(), companyQueryRepository.countAll()
        ).map(tuple -> {
            var content = tuple.getT1();
            var total = tuple.getT2();
            return new PageDTO<>(content, page, size, total);
        });
    }
}
