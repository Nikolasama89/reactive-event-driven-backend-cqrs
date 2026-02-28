package com.itsaur.model;

import java.time.OffsetDateTime;

public class CompanyQueryModel {
    private Long id;
    private String companyName;
    private String vat;
    private String address;
    private Long totalEmployees;
    private OffsetDateTime createdAt;
    private OffsetDateTime updatedAt;

    public CompanyQueryModel(){}

    public CompanyQueryModel(Long id, String companyName, String vat, String address, Long totalEmployees, OffsetDateTime createdAt, OffsetDateTime updatedAt) {
        this.id = id;
        this.companyName = companyName;
        this.vat = vat;
        this.address = address;
        this.totalEmployees = totalEmployees;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public String getVat() {
        return vat;
    }

    public void setVat(String vat) {
        this.vat = vat;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public Long getTotalEmployees() {
        return totalEmployees;
    }

    public void setTotalEmployees(Long totalEmployees) {
        this.totalEmployees = totalEmployees;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(OffsetDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public OffsetDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(OffsetDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
}
