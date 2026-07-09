package com.training.inMemory.search;

public interface InMemoryConditionStrategy {
    boolean checkValue(Object fieldValue, Object searchValue);
}
