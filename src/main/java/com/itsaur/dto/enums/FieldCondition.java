package com.itsaur.dto.enums;

public enum FieldCondition {
    FIRSTNAME("firstname"),
    LASTNAME("lastname"),
    EMAIL("email"),
    COMPANY_NAME("company_name"),
    ;

    private final String column;

    private FieldCondition(String column) {
        this.column = column;
    }

    public String getColumn() {
        return column;
    }

}
