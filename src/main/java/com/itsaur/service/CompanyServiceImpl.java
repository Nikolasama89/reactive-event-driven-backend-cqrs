package com.itsaur.service;

import com.itsaur.dto.CompanyCreateDTO;
import com.itsaur.dto.CompanyUpdateDTO;
import com.itsaur.event.*;
import com.itsaur.exceptions.*;
import com.itsaur.model.*;
import com.itsaur.repository.CompanyRepository;
import reactor.core.publisher.Mono;


public class CompanyServiceImpl implements CompanyService {

    private final CompanyRepository companyRepository;
    private final EventPublisher eventPublisher;

    public CompanyServiceImpl(CompanyRepository companyRepository, EventPublisher eventPublisher) {
        this.companyRepository = companyRepository;
        this.eventPublisher = eventPublisher;

    }

    @Override
    public Mono<Company> findCompanyById(Long id) {
        if (id == null || id <= 0 ){
            return Mono.error(new EntityInvalidArgumentException("Id can not be null or empty"));
        }
        return companyRepository.getCompanyById(id)
                .switchIfEmpty(Mono.error(new CompanyNotFoundException("Company with id: " +  id + " not found.")));
    }

    @Override
    public Mono<Company> save(CompanyCreateDTO companyCreateDTO) {

        if (companyCreateDTO.getVat() == null){
            return Mono.error(new EntityInvalidArgumentException("Company VAT cannot be null"));
        }
        return companyRepository.existsVat(companyCreateDTO.getVat())
                .flatMap(exists -> {
                    if (exists) {
                        return Mono.error(new CompanyVatAlreadyExistsException("VAT already exists."));
                    }

                    Company company = Company.create(
                            companyCreateDTO.getCompanyName(),
                            companyCreateDTO.getVat(),
                            companyCreateDTO.getAddress()
                    );

                    CompanyCreatedEvent event = new CompanyCreatedEvent(
                            company.getId(),
                            company.getCompanyName(),
                            company.getVat(),
                            company.getAddress(),
                            company.getCreatedAt()
                    );
                    return eventPublisher.publish(event).thenReturn(company);
                });
    }

    @Override
    public Mono<Void> update(Long id, CompanyUpdateDTO companyUpdateDTO) {

        if (id == null || id <= 0) {
            return Mono.error(new EntityInvalidArgumentException("Id must be greater than zero"));
        }
        if (companyUpdateDTO.getVat() == null){
            return Mono.error(new EntityInvalidArgumentException("Company VAT cannot be null"));
        }
        return companyRepository.getCompanyById(id)
                .switchIfEmpty(Mono.error(new CompanyNotFoundException("Company with id: " + id + " not found")))
                .flatMap(existing -> {

                    Company updated = existing.update(
                            companyUpdateDTO.getCompanyName(),
                            companyUpdateDTO.getVat(),
                            companyUpdateDTO.getAddress());

                    CompanyUpdatedEvent event = new CompanyUpdatedEvent(
                            updated.getId(),
                            updated.getCompanyName(),
                            updated.getVat(),
                            updated.getAddress(),
                            updated.getUpdatedAt()
                    );
                    return eventPublisher.publish(event);
                });
    }

    @Override
    public Mono<Void> delete(Long id) {
        if (id == null || id <= 0){
            return Mono.error(new EntityInvalidArgumentException("Id must be greater than zero"));
        }
        return companyRepository.getCompanyById(id)
                .switchIfEmpty(Mono.error(new CompanyNotFoundException("Company with id: " + id + " not found.")))
                .flatMap(company -> eventPublisher.publish(new CompanyDeletedEvent(CompanyId.create(id)))).then();
    }
}