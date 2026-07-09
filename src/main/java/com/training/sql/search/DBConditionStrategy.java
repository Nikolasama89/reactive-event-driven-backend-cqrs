package com.training.sql.search;

public interface DBConditionStrategy {
    String buildSearch(String column, int index);
    Object searchValue(String value);
}
