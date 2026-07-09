package com.training.inMemory.repository;

import com.training.repository.search.SearchComponent;
import com.training.model.Company;
import com.training.model.CompanyQueryModel;
import com.training.repository.CompanyQueryRepository;
import com.training.inMemory.search.InMemoryCompanySearch;
import com.training.inMemory.search.InMemoryStorage;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Predicate;

public class InMemoryCompanyQueryRepository implements CompanyQueryRepository {

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
        model.setId(company.getId().getValue());
        model.setCompanyName(company.getCompanyName().getValue());
        model.setVat(company.getVat().getValue());
        model.setAddress(company.getAddress().getValue());
        model.setTotalEmployees(totalEmployees);
        model.setCreatedAt(company.getCreatedAt());
        model.setUpdatedAt(company.getUpdatedAt());

        return Mono.just(model);
    }
}
