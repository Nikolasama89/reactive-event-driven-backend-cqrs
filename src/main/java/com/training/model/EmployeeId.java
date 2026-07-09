package com.training.model;

import java.security.SecureRandom;
import java.util.Objects;

public class EmployeeId {
    private final Long value;

    private EmployeeId(Long value){
        this.value = value;
    }

    public static EmployeeId create(Long value){
        if (value == null){
            return null;
        }
        return new EmployeeId(value);
    }

    public static EmployeeId construct(Long value){
        if (value == null){
            return null;
        }
        return new EmployeeId(value);
    }

    public Long getValue() {
        return value;
    }

    public static EmployeeId generateId(){
        SecureRandom secureRandom = new SecureRandom();
        Long id = Math.abs(secureRandom.nextLong());
        return new EmployeeId(id);
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EmployeeId that = (EmployeeId) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }
}