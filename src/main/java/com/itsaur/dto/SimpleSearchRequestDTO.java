package com.itsaur.dto;

import com.itsaur.dto.enums.FieldCondition;
import com.itsaur.dto.enums.TypeCondition;
import com.itsaur.repository.search.DBConditionStrategy;
import com.itsaur.repository.search.DBConditionStrategyFactory;

import java.util.List;

public class SimpleSearchRequestDTO implements SearchComponent {
    private TypeCondition type;
    private FieldCondition field;
    private String value;

    public SimpleSearchRequestDTO() {
    }

    public SimpleSearchRequestDTO(TypeCondition type, FieldCondition field, String value) {
        this.type = type;
        this.field = field;
        this.value = value;
    }

    public TypeCondition getType() {
        return type;
    }

    public void setType(TypeCondition type) {
        this.type = type;
    }

    public FieldCondition getField() {
        return field;
    }

    public void setField(FieldCondition field) {
        this.field = field;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public void sqlBuild(StringBuilder sqlString, List<String> params) {
        DBConditionStrategy strategy = DBConditionStrategyFactory.getStrategy(type);

        int index = params.size();
        params.add(strategy.searchValue(value));

        sqlString.append(strategy.buildSearch(field.getColumn(), index + 1));
    }
}