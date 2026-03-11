package com.itsaur.repository;

import com.itsaur.dto.ComplexSearchRequestDTO;
import com.itsaur.dto.SearchComponent;
import com.itsaur.dto.SimpleSearchRequestDTO;
import com.itsaur.dto.enums.EmployeeSearchFieldCondition;
import com.itsaur.dto.enums.LogicOperator;
import com.itsaur.model.EmployeeQueryModel;
import com.itsaur.repository.search.InMemoryConditionStrategy;
import com.itsaur.repository.search.InMemoryConditionStrategyFactory;
import com.itsaur.repository.search.SearchHandler;

import java.util.List;
import java.util.function.Predicate;

public class InMemoryEmployeeSearch implements SearchHandler<Predicate<EmployeeQueryModel>> {

    private final InMemoryStorage storage;

    public InMemoryEmployeeSearch(InMemoryStorage storage){
        this.storage = storage;
    }

    @Override
    public Predicate<EmployeeQueryModel> handle(SearchComponent root) {
        return buildPredicate(root);
    }

    private Predicate<EmployeeQueryModel> buildPredicate(SearchComponent component) {

        if (component instanceof SimpleSearchRequestDTO simple) {
            return buildSimplePredicate(simple);
        }

        if (component instanceof ComplexSearchRequestDTO complex) {

            List<Predicate<EmployeeQueryModel>> predicates =
                    complex.getConditions()
                            .stream()
                            .map(this::buildPredicate)
                            .toList();

            if (complex.getType() == LogicOperator.AND) {
                return predicates.stream()
                        .reduce(e -> true, Predicate::and);
            }

            if (complex.getType() == LogicOperator.OR) {
                return predicates.stream()
                        .reduce(e -> false, Predicate::or);
            }
        }
        return e -> true;
    }

    private Predicate<EmployeeQueryModel> buildSimplePredicate(
            SimpleSearchRequestDTO dto) {

        String value = dto.getValue().toLowerCase();
        InMemoryConditionStrategy strategy = InMemoryConditionStrategyFactory.getStrategy(dto.getType());
        EmployeeSearchFieldCondition employeeField = EmployeeSearchFieldCondition.valueOf(dto.getField().name());

        return employeeQueryModel -> switch (employeeField){

            case FIRSTNAME -> strategy.checkValue(employeeQueryModel.getFirstName().toLowerCase(), value);

            case LASTNAME -> strategy.checkValue(employeeQueryModel.getLastName().toLowerCase(), value);

            case EMAIL -> strategy.checkValue(employeeQueryModel.getEmail().toLowerCase(), value);

            case COMPANY_NAME -> storage.getCompanies().values().stream()
                    .filter(c -> c.getId().equals(employeeQueryModel.getCompanyId()))
                    .anyMatch(c -> strategy.checkValue(c.getCompanyName().toLowerCase(), value));
        };
    }
}
