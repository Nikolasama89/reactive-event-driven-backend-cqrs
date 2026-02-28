package com.itsaur.service;

import com.itsaur.dto.CompanyCreateDTO;
import com.itsaur.dto.CompanyUpdateDTO;
import com.itsaur.dto.PageDTO;
import com.itsaur.model.Company;
import com.itsaur.model.CompanyQueryModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface CompanyService {
    Mono<Company> save(CompanyCreateDTO companyCreateDTO);
    Mono<Void> update(Long id, CompanyUpdateDTO companyUpdateDTO);
    Mono<Void> delete(Long id);
    Mono<Company> findCompanyById(Long id);
}
