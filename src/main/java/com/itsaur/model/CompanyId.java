package com.itsaur.model;

import java.security.SecureRandom;
import java.util.Objects;

public class CompanyId {
    private final Long value;

    private CompanyId(Long value){
        this.value = value;
    }

    public static CompanyId create(Long value){
        if (value == null){
            return null;
        }

        return new CompanyId(value);
    }

    public static CompanyId construct(Long value){
        if (value == null){
            return null;
        }
        return new CompanyId(value);
    }

    public Long getValue() {
        return value;
    }

    public static CompanyId generateId(){
        SecureRandom secureRandom = new SecureRandom();
        Long id = Math.abs(secureRandom.nextLong());
        return new CompanyId(id);
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompanyId companyId = (CompanyId) o;
        return Objects.equals(value, companyId.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }
}