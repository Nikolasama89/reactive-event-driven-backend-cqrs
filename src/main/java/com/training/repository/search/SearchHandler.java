package com.training.repository.search;

public interface SearchHandler<T> {
    T handle(SearchComponent root);
}
