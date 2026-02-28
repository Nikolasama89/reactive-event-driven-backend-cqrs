package com.itsaur.dto.enums;

public enum LogicOperator {
    AND("and"),
    OR("or"),
    ;

    private final String text;

    private LogicOperator(String text) {
        this.text = text;
    }

    public String getText() {
        return  text;
    }
}
