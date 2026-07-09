package com.training.service;

import com.training.repository.search.ComplexSearchRequestDTO;
import com.training.dto.PageDTO;
import com.training.repository.search.SearchComponent;
import com.training.repository.search.SimpleSearchRequestDTO;
import com.training.exceptions.CompanyNotFoundException;
import com.training.exceptions.EntityInvalidArgumentException;
import com.training.exceptions.SearchValueCharacterLengthException;
import com.training.exceptions.SearchValueEmptyException;
import com.training.model.CompanyQueryModel;
import com.training.repository.CompanyQueryRepository;
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

    @Override
    public Mono<PageDTO<CompanyQueryModel>> getPaginatedSearchCompanies(SearchComponent searchComponent, int page, int size) {
        if (page < 0 || size <= 0) {
            return Mono.error(new EntityInvalidArgumentException("Page and size cannot be null"));
        }

        if (searchComponent instanceof SimpleSearchRequestDTO simpleSearchRequestDTO){
            String value = simpleSearchRequestDTO.getValue();

            if (value == null || value.trim().isEmpty()) {
                return Mono.error(new SearchValueEmptyException("Search value can not be empty"));
            }

            if (value.length() > 50) {
                return Mono.error(new SearchValueCharacterLengthException("Value must be 1 - 50 letters max."));
            }
        } else if (searchComponent instanceof ComplexSearchRequestDTO complexRequestDto) {
            if (complexRequestDto.getConditions() == null || complexRequestDto.getConditions().isEmpty()){
                return Mono.error(new EntityInvalidArgumentException("Conditions can not be empty"));
            }
            for (SearchComponent child: complexRequestDto.getConditions()){
                if (child instanceof SimpleSearchRequestDTO simpleChild){
                    String value = simpleChild.getValue();
                    if (value == null || value.trim().isEmpty()) {
                        return Mono.error(new SearchValueEmptyException("Search value can not be empty"));
                    }

                    if (value.length() > 50) {
                        return Mono.error(new SearchValueCharacterLengthException("Value must be 1 - 50 letters max."));
                    }
                }
            }
        }

        if (searchComponent == null){
            return Mono.zip(
                    companyQueryRepository.findPage(page, size).collectList(), companyQueryRepository.countAll()
            ).map(tuple -> {
                var content = tuple.getT1();
                var total = tuple.getT2();
                return new PageDTO<>(content, page, size, total);
            });
        } else {
            return Mono.zip(companyQueryRepository.paginatedCompanyFilterQuery(searchComponent, page, size).collectList(), companyQueryRepository.countViewCompanies(searchComponent))
                    .map(tuple -> {
                        var content = tuple.getT1();
                        var total = tuple.getT2();
                        return new PageDTO<>(content, page, size, total);
                    });
        }


    }
}
