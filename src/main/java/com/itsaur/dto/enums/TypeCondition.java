package com.itsaur.dto.enums;

public enum TypeCondition {
    EQUALS("equals"),
    CONTAINS("contains"),
    STARTSWITH("startsWith"),
    ENDSWITH("endsWith"),
    ;

    private final String text;

    TypeCondition(String text) {
        this.text = text;
    }


    public String getText() {
        return text;
    }
}
