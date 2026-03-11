package com.itsaur.repository;

import com.itsaur.dto.SearchComponent;
import com.itsaur.model.Company;
import com.itsaur.model.CompanyQueryModel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Predicate;

public class InMemoryCompanyQueryRepository implements CompanyQueryRepository{

    private final InMemoryStorage inMemoryStorage;
    private final InMemoryCompanySearch searchCompiler;

    public InMemoryCompanyQueryRepository(InMemoryStorage inMemoryStorage, InMemoryCompanySearch searchCompiler){
        this.inMemoryStorage = inMemoryStorage;
        this.searchCompiler = searchCompiler;
    }

    @Override
    public Mono<CompanyQueryModel> getCompanyById(Long id) {
        return Mono.justOrEmpty(inMemoryStorage.companies.get(id)).flatMap(this::toQueryModel);
    }

    @Override
    public Flux<CompanyQueryModel> getAllCompanies() {
        return Flux.fromIterable(inMemoryStorage.getCompanies().values()).flatMap(this::toQueryModel);
    }

    @Override
    public Flux<CompanyQueryModel> findPage(Integer page, Integer size) {
        long offset = (long) page * size;

        return getAllCompanies()
                .skip(offset)
                .take(size);
    }

    @Override
    public Mono<Long> countAll() {
        return getAllCompanies().count();
    }

    @Override
    public Flux<CompanyQueryModel> paginatedCompanyFilterQuery(SearchComponent searchComponent, Integer page, Integer size) {
        Predicate<CompanyQueryModel> predicate = searchCompiler.handle(searchComponent);

        return getAllCompanies().filter(predicate).skip((long) page * size).take(size);
    }

    @Override
    public Mono<Long> countViewCompanies(SearchComponent searchComponent) {
        Predicate<CompanyQueryModel> predicate = searchCompiler.handle(searchComponent);

        return getAllCompanies().filter(predicate).count();
    }

    private Mono<CompanyQueryModel> toQueryModel(Company company) {

        long totalEmployees = inMemoryStorage.getEmployees()
                .values()
                .stream()
                .filter(e -> e.getCompanyId() != null &&
                        e.getCompanyId().equals(company.getId()))
                .count();


        CompanyQueryModel model = new CompanyQueryModel();
        model.setId(company.getId());
        model.setCompanyName(company.getCompanyName());
        model.setVat(company.getVat());
        model.setAddress(company.getAddress());
        model.setTotalEmployees(totalEmployees);
        model.setCreatedAt(company.getCreatedAt());
        model.setUpdatedAt(company.getUpdatedAt());

        return Mono.just(model);
    }
}
