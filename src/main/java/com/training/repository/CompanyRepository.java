package com.training.repository;

import com.training.model.Company;
import reactor.core.publisher.Mono;

public interface CompanyRepository {
    Mono<Company> getCompanyById(Long id);
    Mono<Boolean> existsVat(String vat);
}
