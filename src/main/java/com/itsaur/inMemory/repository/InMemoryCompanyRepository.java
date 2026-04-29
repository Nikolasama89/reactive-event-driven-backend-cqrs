package com.itsaur.inMemory.repository;

import com.itsaur.model.Company;
import com.itsaur.repository.CompanyRepository;
import com.itsaur.inMemory.search.InMemoryStorage;
import reactor.core.publisher.Mono;

import java.time.OffsetDateTime;

public class InMemoryCompanyRepository implements CompanyRepository {

    private final InMemoryStorage inMemoryStorage;

    public InMemoryCompanyRepository(InMemoryStorage inMemoryStorage){
        this.inMemoryStorage = inMemoryStorage;
    }


    @Override
    public Mono<Company> getCompanyById(Long id) {
        return Mono.justOrEmpty(inMemoryStorage.getCompanies().get(id));
    }

    @Override
    public Mono<Company> createOrUpdateCompany(Long id, Company company) {
        if (id == null){
            long newId = inMemoryStorage.getIdGenerator().incrementAndGet();
            inMemoryStorage.getCompanies().put(newId, company);
            return Mono.just(company);
        }
        return Mono.justOrEmpty(inMemoryStorage.getCompanies().get(id))
                .map(existing -> {
                    Company updated = Company.create(
                            existing.getCompanyName().getValue(),
                            existing.getVat().getValue(),
                            existing.getAddress().getValue());

                    inMemoryStorage.getCompanies().put(id, updated);
                    return updated;
                });
    }

    @Override
    public Mono<Void> deleteCompany(Long id) {
        inMemoryStorage.getCompanies().remove(id);
        return Mono.empty();
    }

    @Override
    public Mono<Boolean> existsVat(String vat) {
        boolean exists = inMemoryStorage.getCompanies().values().stream().anyMatch(company ->
                company.getVat() != null && company.getVat().getValue().equalsIgnoreCase(vat));
        return Mono.just(exists);
    }
}
