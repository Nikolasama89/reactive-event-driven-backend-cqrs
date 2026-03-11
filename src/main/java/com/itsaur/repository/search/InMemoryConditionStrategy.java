package com.itsaur.repository.search;

public interface InMemoryConditionStrategy {
    boolean checkValue(Object fieldValue, Object searchValue);
}
