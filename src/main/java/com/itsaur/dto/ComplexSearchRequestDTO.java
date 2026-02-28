package com.itsaur.dto;

import com.itsaur.dto.enums.LogicOperator;

import java.util.ArrayList;
import java.util.List;

public class ComplexSearchRequestDTO implements SearchComponent {
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

    @Override
    public void sqlBuild(StringBuilder sqlString, List<String> params) {

        String joiner = type == LogicOperator.AND  ? " AND " : " OR ";
        List<String> parts = new ArrayList<>();

        for (SearchComponent condition : conditions) {
            StringBuilder builder = new StringBuilder();
            condition.sqlBuild(builder, params);
            parts.add(builder.toString());
        }
        sqlString.append("(");
        sqlString.append(String.join(joiner, parts));
        sqlString.append(")");
    }
}
