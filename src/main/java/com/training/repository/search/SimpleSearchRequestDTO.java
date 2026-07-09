package com.training.repository.search;

import com.training.dto.enums.SearchFieldCondition;
import com.training.dto.enums.TypeCondition;

public class SimpleSearchRequestDTO implements SearchComponent {
    private TypeCondition type;
    private SearchFieldCondition searchField;
    private String value;

    public SimpleSearchRequestDTO() {
    }

    public SimpleSearchRequestDTO(TypeCondition type, SearchFieldCondition searchField, String value) {
        this.type = type;
        this.searchField = searchField;
        this.value = value;
    }

    public TypeCondition getType() {
        return type;
    }

    public void setType(TypeCondition type) {
        this.type = type;
    }

    public SearchFieldCondition getSearchField() {
        return searchField;
    }

    public void setSearchField(SearchFieldCondition searchField) {
        this.searchField = searchField;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}