package com.itsaur.inMemory.search;

import com.itsaur.repository.search.ComplexSearchRequestDTO;
import com.itsaur.repository.search.SearchComponent;
import com.itsaur.repository.search.SimpleSearchRequestDTO;
import com.itsaur.dto.enums.CompanySearchFieldCondition;
import com.itsaur.dto.enums.LogicOperator;
import com.itsaur.model.CompanyQueryModel;
import com.itsaur.repository.search.SearchHandler;

import java.util.List;
import java.util.function.Predicate;

public class InMemoryCompanySearch implements SearchHandler<Predicate<CompanyQueryModel>> {
    private final InMemoryStorage storage;

    public InMemoryCompanySearch(InMemoryStorage storage) {
        this.storage = storage;
    }

    @Override
    public Predicate<CompanyQueryModel> handle(SearchComponent root) {
        return buildPredicate(root);
    }

    private Predicate<CompanyQueryModel> buildPredicate(SearchComponent searchComponent){

        if (searchComponent instanceof SimpleSearchRequestDTO simple){
            return buildSimplePredicate(simple);
        }

        if (searchComponent instanceof ComplexSearchRequestDTO complex){
            List<Predicate<CompanyQueryModel>> predicates = complex.getConditions()
                    .stream()
                    .map(this::buildPredicate)
                    .toList();

            if (complex.getType() == LogicOperator.AND){
                return predicates.stream().reduce(e -> true, Predicate::and);
            }

            if (complex.getType() == LogicOperator.OR){
                return predicates.stream().reduce(e -> false, Predicate::or);
            }
        }
        return e -> true;
    }

    private Predicate<CompanyQueryModel> buildSimplePredicate(SimpleSearchRequestDTO dto){

        String value = dto.getValue().toLowerCase();
        InMemoryConditionStrategy strategy =
                InMemoryConditionStrategyFactory.getStrategy(dto.getType());
        CompanySearchFieldCondition companySearchFieldCondition = CompanySearchFieldCondition.valueOf(dto.getSearchField().name());

        return company -> switch (companySearchFieldCondition) {

            case COMPANY_NAME ->
                    strategy.checkValue(company.getCompanyName().toLowerCase(), value);

            case FIRSTNAME ->
                    storage.getEmployees().values().stream()
                            .filter(e -> e.getCompanyId().equals(company.getId()))
                            .anyMatch(e ->
                                    strategy.checkValue(
                                            e.getFullName().getFirstname().toLowerCase(),
                                            value));

            case LASTNAME ->
                    storage.getEmployees().values().stream()
                            .filter(e -> e.getCompanyId().equals(company.getId()))
                            .anyMatch(e ->
                                    strategy.checkValue(
                                            e.getFullName().getLastname().toLowerCase(),
                                            value));
        };
    }
}