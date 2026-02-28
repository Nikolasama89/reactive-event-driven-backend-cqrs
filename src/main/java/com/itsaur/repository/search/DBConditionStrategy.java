package com.itsaur.repository.search;

public interface DBConditionStrategy {
    String buildSearch(String column, int index);
    String searchValue(String value);
}
