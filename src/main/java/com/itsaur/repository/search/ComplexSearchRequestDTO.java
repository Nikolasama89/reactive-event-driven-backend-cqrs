package com.itsaur.repository.search;

import com.itsaur.dto.enums.LogicOperator;

import java.util.List;

public class ComplexSearchRequestDTO implements SearchComponent{
    private LogicOperator type;
    private List<SearchComponent> conditions;

    public ComplexSearchRequestDTO() {
    }

    public ComplexSearchRequestDTO(LogicOperator type, List<SearchComponent> conditions) {
        this.type = type;
        this.conditions = conditions;
    }

    public LogicOperator getType() {
        return type;
    }

    public void setType(LogicOperator type) {
        this.type = type;
    }

    public List<SearchComponent> getConditions() {
        return conditions;
    }

    public void setConditions(List<SearchComponent> conditions) {
        this.conditions = conditions;
    }
}