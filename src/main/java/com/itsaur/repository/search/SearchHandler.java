package com.itsaur.repository.search;

import com.itsaur.dto.SearchComponent;

public interface SearchHandler<T> {
    T handle(SearchComponent root);
}
