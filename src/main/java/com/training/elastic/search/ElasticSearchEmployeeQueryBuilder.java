package com.training.elastic.search;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import com.training.repository.search.ComplexSearchRequestDTO;
import com.training.repository.search.SearchComponent;
import com.training.repository.search.SimpleSearchRequestDTO;
import com.training.dto.enums.SearchFieldCondition;

import java.util.List;

public class ElasticSearchEmployeeQueryBuilder {

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

        String field = resolveField(simple.getSearchField());
        String keywordField = field + ".keyword";
        String value = simple.getValue();

        return switch (simple.getType()) {

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

    private String resolveField(SearchFieldCondition searchFieldCondition) {
        return switch (searchFieldCondition) {
            case FIRSTNAME -> "firstname";
            case LASTNAME -> "lastname";
            case EMAIL -> "email";
            case COMPANY_NAME -> "companyName";
        };
    }
}