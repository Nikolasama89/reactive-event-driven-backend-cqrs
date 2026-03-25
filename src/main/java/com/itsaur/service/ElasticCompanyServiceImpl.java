package com.itsaur.service;

import com.itsaur.dto.CompanyCreateDTO;
import com.itsaur.dto.CompanyUpdateDTO;
import com.itsaur.event.CompanyCreatedEvent;
import com.itsaur.event.CompanyDeletedEvent;
import com.itsaur.event.CompanyUpdatedEvent;
import com.itsaur.event.EventPublisher;
import com.itsaur.exceptions.*;
import com.itsaur.mapper.CompanyMapper;
import com.itsaur.model.Company;
import com.itsaur.repository.CompanyRepository;
import com.itsaur.repository.ElasticCompanyRepository;
import reactor.core.publisher.Mono;

public class ElasticCompanyServiceImpl implements CompanyService{
    private final CompanyRepository companyRepository;
    private final ElasticCompanyRepository elasticCompanyRepository;
    private final CompanyMapper companyMapper = new CompanyMapper();
    private final EventPublisher eventPublisher;

    public ElasticCompanyServiceImpl(CompanyRepository companyRepository, ElasticCompanyRepository elasticCompanyRepository, EventPublisher eventPublisher) {
        this.companyRepository = companyRepository;
        this.elasticCompanyRepository = elasticCompanyRepository;
        this.eventPublisher = eventPublisher;
    }

    @Override
    public Mono<Company> findCompanyById(Long id) {
        if (id == 0 || id < 0){
            return Mono.error(new EntityInvalidArgumentException("Id can not be null or empty"));
        }
        return companyRepository.getCompanyById(id)
                .switchIfEmpty(Mono.error(new CompanyNotFoundException("Company with id: " +  id + " not found.")));
    }

    @Override
    public Mono<Company> save(CompanyCreateDTO companyCreateDTO) {
        if (companyCreateDTO.getCompanyName() == null ||  companyCreateDTO.getCompanyName().isEmpty()) {
            return Mono.error(new CompanyNameRequiredException("Company name is required."));
        } else if (companyCreateDTO.getCompanyName().length() < 3 || companyCreateDTO.getCompanyName().length() > 50) {
            return Mono.error(new CompanyInvalidVatLengthException("Company name length should be between 3 and 50 characters."));
        }
        if (companyCreateDTO.getVat() == null || companyCreateDTO.getVat().isEmpty()) {
            return Mono.error(new CompanyVatRequiredException("Company VAT is required."));
        }
        if (!(companyCreateDTO.getVat().matches("^\\d{10}$"))){
            return Mono.error(new CompanyVatInvalidInputException("VAT should be 10 numbers"));
        }
        if (companyCreateDTO.getAddress() == null || companyCreateDTO.getAddress().isEmpty()) {
            return Mono.error(new CompanyAddressRequiredException("Company address is required."));
        }

        Company company = companyMapper.toEntity(companyCreateDTO);
        return companyRepository.existsVat(company.getVat())
                .flatMap(exists -> {
                    if (exists) {
                        return Mono.error(new CompanyVatAlreadyExistsException("VAT already exists."));
                    }
                    return companyRepository.createOrUpdateCompany(null, company)
                            .flatMap(createdCompany -> eventPublisher
                                    .publish(new CompanyCreatedEvent(createdCompany.getId()))
                                    .thenReturn(createdCompany));
                });

    }

    @Override
    public Mono<Void> update(Long id, CompanyUpdateDTO companyUpdateDTO) {
        if (companyUpdateDTO.getCompanyName() == null ||  companyUpdateDTO.getCompanyName().isEmpty()) {
            return Mono.error(new CompanyNameRequiredException("Company name is required."));
        } else if (!(companyUpdateDTO.getVat().length() == 10)) {
            return Mono.error(new CompanyInvalidVatLengthException("Company vat should be 10 characters."));
        }

        if (companyUpdateDTO.getAddress() == null || companyUpdateDTO.getAddress().isEmpty()) {
            return Mono.error(new CompanyAddressRequiredException("Company address is required."));
        }

        return companyRepository.getCompanyById(id)
                .switchIfEmpty(Mono.error(new CompanyNotFoundException("Company with id: " + id + " not found")))
                .flatMap(existing -> {
                    existing.setCompanyName(companyUpdateDTO.getCompanyName());
                    existing.setVat(companyUpdateDTO.getVat());
                    existing.setAddress(companyUpdateDTO.getAddress());
                    return companyRepository.createOrUpdateCompany(id, existing).flatMap(updatedCompany ->
                                    elasticCompanyRepository.createOrUpdateCompany(updatedCompany.getId(), updatedCompany))
                            .flatMap(updatedCompany -> eventPublisher
                                    .publish(new CompanyUpdatedEvent(updatedCompany.getId())));
                })
                .then();
    }

    @Override
    public Mono<Void> delete(Long id) {
        if (id == null || id <= 0){
            return Mono.error(new EntityInvalidArgumentException("Id can not be null or empty"));
        }
        return companyRepository.getCompanyById(id)
                .switchIfEmpty(Mono.error(new CompanyNotFoundException("Company with id: " + id + " not found")))
                .flatMap(company ->
                        companyRepository.deleteCompany(id)
                                .then(eventPublisher.publish(new CompanyDeletedEvent(company.getId())))
                );
    }
}
