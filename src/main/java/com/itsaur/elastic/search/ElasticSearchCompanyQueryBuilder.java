package com.itsaur.elastic.search;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import com.itsaur.repository.search.ComplexSearchRequestDTO;
import com.itsaur.repository.search.SearchComponent;
import com.itsaur.repository.search.SimpleSearchRequestDTO;
import com.itsaur.dto.enums.CompanySearchFieldCondition;
import com.itsaur.dto.enums.SearchFieldCondition;

import java.util.List;

public class ElasticSearchCompanyQueryBuilder {
    public Query build(SearchComponent searchComponent){
        if (searchComponent instanceof SimpleSearchRequestDTO simple) {
            return buildSimple(simple);
        }

        if (searchComponent instanceof ComplexSearchRequestDTO complex) {
            return buildComplex(complex);
        }

        throw new IllegalArgumentException("Unknown component type");
    }

    private Query buildSimple(SimpleSearchRequestDTO simple) {

        CompanySearchFieldCondition fieldCondition = mapToCompanyField(simple.getSearchField());
        String keywordField = resolveField(fieldCondition) + ".keyword";
        String value = simple.getValue();

        Query baseQuery = switch (simple.getType()) {

            case EQUALS -> Query.of(q -> q.term(t -> t
                    .field(keywordField)
                    .value(value)
                    .caseInsensitive(true)
            ));

            case CONTAINS -> Query.of(q -> q.wildcard(w -> w
                    .field(keywordField)
                    .value("*" + value.toLowerCase() + "*")
                    .caseInsensitive(true)
            ));

            case STARTSWITH -> Query.of(q -> q.wildcard(w -> w
                    .field(keywordField)
                    .value(value.toLowerCase() + "*")
                    .caseInsensitive(true)
            ));

            case ENDSWITH -> Query.of(q -> q.wildcard(w -> w
                    .field(keywordField)
                    .value("*" + value.toLowerCase())
                    .caseInsensitive(true)
            ));
        };

        if (isNested(fieldCondition)) {
            return Query.of(q -> q.nested(n -> n
                    .path("employees")
                    .query(baseQuery)
            ));
        }

        return baseQuery;
    }


    private Query buildComplex(ComplexSearchRequestDTO complex) {

        List<Query> queries = complex.getConditions()
                .stream()
                .map(this::build)
                .toList();

        return Query.of(q -> q.bool(b -> {

            switch (complex.getType()) {
                case AND -> b.must(queries);
                case OR -> b.should(queries);
            }

            return b;
        }));

    }

    private CompanySearchFieldCondition mapToCompanyField(SearchFieldCondition field) {
        return switch (field) {
            case COMPANY_NAME -> CompanySearchFieldCondition.COMPANY_NAME;
            case FIRSTNAME -> CompanySearchFieldCondition.FIRSTNAME;
            case LASTNAME -> CompanySearchFieldCondition.LASTNAME;
            default -> throw new IllegalArgumentException("Field " + field + " is not valid for company search");
        };
    }

    private String resolveField(CompanySearchFieldCondition fieldCondition) {
        return switch (fieldCondition) {
            case FIRSTNAME -> "employees.firstname";
            case LASTNAME -> "employees.lastname";
            case COMPANY_NAME -> "companyName";
        };
    }

    private boolean isNested(CompanySearchFieldCondition fieldCondition) {
        return switch (fieldCondition) {
            case FIRSTNAME, LASTNAME -> true;
            case COMPANY_NAME -> false;
        };
    }
}