package com.training.dto;

import java.util.List;

public class PageDTO<T> {

    private final List<T> items;
    private final int page;
    private final int size;
    private final long totalElements;

    public PageDTO(List<T> items, int page, int size, long totalElements) {
        this.items = items;
        this.page = page;
        this.size = size;
        this.totalElements = totalElements;
    }

    public List<T> getItems() {
        return items;
    }

    public int getPage() {
        return page;
    }

    public int getSize() {
        return size;
    }

    public long getTotalElements() {
        return totalElements;
    }

    public long getTotalPages() {
        if (size == 0) return 0;
        return (totalElements + size - 1) / size;
    }

    public boolean isHasNext() {
        return page + 1 < getTotalPages();
    }
}
