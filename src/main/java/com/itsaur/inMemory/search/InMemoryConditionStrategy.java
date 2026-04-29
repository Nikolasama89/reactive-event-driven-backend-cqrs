package com.itsaur.inMemory.search;

public interface InMemoryConditionStrategy {
    boolean checkValue(Object fieldValue, Object searchValue);
}
