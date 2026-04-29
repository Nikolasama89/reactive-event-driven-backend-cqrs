package com.itsaur.inMemory.search;

import com.itsaur.model.Company;
import com.itsaur.model.Employee;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryStorage {
    public final Map<Long, Employee> employees = new ConcurrentHashMap<>();
    public final Map<Long, Company> companies = new ConcurrentHashMap<>();
    private final AtomicLong idGenerator = new AtomicLong();

    public Map<Long, Employee> getEmployees() {
        return employees;
    }

    public AtomicLong getIdGenerator() {
        return idGenerator;
    }

    public Map<Long, Company> getCompanies() {
        return companies;
    }
}
