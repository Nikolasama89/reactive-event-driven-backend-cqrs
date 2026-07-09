package com.training.service;

import com.training.dto.CompanyCreateDTO;
import com.training.dto.CompanyUpdateDTO;
import com.training.model.Company;
import reactor.core.publisher.Mono;

public interface CompanyService {
    Mono<Company> save(CompanyCreateDTO companyCreateDTO);
    Mono<Void> update(Long id, CompanyUpdateDTO companyUpdateDTO);
    Mono<Void> delete(Long id);
    Mono<Company> findCompanyById(Long id);
}
