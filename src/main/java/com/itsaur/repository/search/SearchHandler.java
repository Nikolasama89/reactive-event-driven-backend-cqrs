package com.itsaur.repository.search;

public interface SearchHandler<T> {
    T handle(SearchComponent root);
}
