package com.training.sql.search;

import com.training.repository.search.ComplexSearchRequestDTO;
import com.training.repository.search.SearchComponent;
import com.training.repository.search.SimpleSearchRequestDTO;
import com.training.dto.enums.LogicOperator;
import com.training.repository.search.SearchHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SQLSearch implements SearchHandler<SQLFilter> {
    @Override
    public SQLFilter handle(SearchComponent root) {
        if (root == null){
            return new SQLFilter(null, List.of());
        }

        List<Object> params = new ArrayList<>();
        String where = compileNode(root, params);

        if (where == null || where.isBlank()){
            return new SQLFilter(null, List.of());
        }
        return new SQLFilter(where, params);
    }

    private String compileNode(SearchComponent node, List<Object> params){
        Objects.requireNonNull(params);

        if (node instanceof SimpleSearchRequestDTO simple){
            return compileSimple(simple, params);
        }

        if (node instanceof ComplexSearchRequestDTO complex){
            return compileComplex(complex, params);
        }
        throw new IllegalArgumentException("No Search Component");
    }

    private String compileSimple(SimpleSearchRequestDTO simple, List<Object> params){
        if (simple.getType() == null || simple.getSearchField() == null || simple.getValue() == null){
            return null;
        }

        DBConditionStrategy strategy = DBConditionStrategyFactory.getStrategy(simple.getType());

        int index = params.size() + 1;
        params.add(strategy.searchValue(simple.getValue()));

        return strategy.buildSearch(simple.getSearchField().name(), index);
    }

    private String compileComplex(ComplexSearchRequestDTO complex, List<Object> params){
        if (complex.getConditions() == null || complex.getConditions().isEmpty()){
            return null;
        }

        String joiner = complex.getType() == LogicOperator.AND ? " AND " : " OR ";

        List<String> parts = new ArrayList<>();
        for (SearchComponent child: complex.getConditions()){
            String childSql = compileNode(child, params);
            if (childSql != null && !childSql.isBlank()){
                parts.add(childSql);
            }
        }

        if (parts.isEmpty()){
            return null;
        }
        return "(" + String.join(joiner, parts) + ")";
    }
}
