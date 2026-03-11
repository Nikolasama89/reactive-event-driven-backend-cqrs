package com.itsaur.repository.search;

import com.itsaur.dto.ComplexSearchRequestDTO;
import com.itsaur.dto.SearchComponent;
import com.itsaur.dto.SimpleSearchRequestDTO;
import com.itsaur.dto.enums.LogicOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SQLSearch implements SearchHandler<SQLFilter> {

    @Override
    public SQLFilter handle(SearchComponent root) {
        if (root == null){
            return new SQLFilter("", List.of());
        }

        List<Object> params = new ArrayList<>();
        String where = compileNode(root, params);

        // if not conditions
        if (where == null || where.isBlank()){
            return new SQLFilter("", List.of());
        }

        return new SQLFilter(where, params);
    }

    private String compileNode(SearchComponent node, List<Object> params) {
        Objects.requireNonNull(params);

        if (node instanceof SimpleSearchRequestDTO simple) {
            return compileSimple(simple, params);
        }

        if (node instanceof ComplexSearchRequestDTO complex) {
            return compileComplex(complex, params);
        }
        throw new IllegalArgumentException("Unknown SearchComponent: " + node.getClass());
    }

    private String compileSimple(SimpleSearchRequestDTO simple, List<Object> params) {
        if (simple.getType() == null || simple.getField() == null || simple.getValue() == null) {
            return ""; // ή throw αν θες αυστηρό validation
        }

        DBConditionStrategy strategy = DBConditionStrategyFactory.getStrategy(simple.getType());

        int index = params.size() + 1; // $1, $2...
        params.add(strategy.searchValue(simple.getValue()));

        return strategy.buildSearch(simple.getField().getColumn(), index);
    }

    private String compileComplex(ComplexSearchRequestDTO complex, List<Object> params) {
        if (complex.getConditions() == null || complex.getConditions().isEmpty()) {
            return "";
        }

        String joiner = (complex.getType() == LogicOperator.AND) ? " AND " : " OR ";

        List<String> parts = new ArrayList<>();
        for (SearchComponent child : complex.getConditions()) {
            String childSql = compileNode(child, params);
            if (childSql != null && !childSql.isBlank()) {
                parts.add(childSql);
            }
        }

        if (parts.isEmpty()) return "";

        return "(" + String.join(joiner, parts) + ")";
    }
}
