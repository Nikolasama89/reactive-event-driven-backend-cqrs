package com.itsaur.dto;

import com.itsaur.dto.enums.SearchFieldCondition;
import com.itsaur.dto.enums.TypeCondition;

public class SimpleSearchRequestDTO implements SearchComponent {
    private TypeCondition type;
    private SearchFieldCondition field;
    private String value;

    public SimpleSearchRequestDTO() {
    }

    public SimpleSearchRequestDTO(TypeCondition type, SearchFieldCondition field, String value) {
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

    public SearchFieldCondition getField() {
        return field;
    }

    public void setField(SearchFieldCondition field) {
        this.field = field;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}