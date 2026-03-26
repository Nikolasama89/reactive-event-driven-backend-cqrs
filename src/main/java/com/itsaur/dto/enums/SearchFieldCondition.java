package com.itsaur.dto.enums;

public enum SearchFieldCondition {
    FIRSTNAME("firstname"),
    LASTNAME("lastname"),
    EMAIL("email"),
    COMPANY_NAME("company_name"),
    ;

    private final String column;

    private SearchFieldCondition(String column) {
        this.column = column;
    }

    public String getColumn() {
        return column;
    }

}
