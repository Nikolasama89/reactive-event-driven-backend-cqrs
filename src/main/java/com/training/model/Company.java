package com.training.model;


import java.time.OffsetDateTime;

public class Company {
    private final CompanyId id;
    private final CompanyName companyName;
    private final Vat vat;
    private final Address address;
    private final OffsetDateTime createdAt;
    private final OffsetDateTime updatedAt;


    private Company(
            CompanyId id,
            CompanyName companyName,
            Vat vat,
            Address address,
            OffsetDateTime createdAt,
            OffsetDateTime updatedAt
    ) {
        this.id = id;
        this.companyName = companyName;
        this.vat = vat;
        this.address = address;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public static Company create(
            String companyName,
            String vat,
            String address
    ) {
        OffsetDateTime now = OffsetDateTime.now();
        return new Company(
                CompanyId.generateId(),
                CompanyName.create(companyName),
                Vat.create(vat),
                Address.create(address),
                now,
                now
        );
    }

    public Company update(String companyName, String vat, String address){
        return new Company(
                this.id,
                CompanyName.create(companyName),
                Vat.create(vat),
                Address.create(address),
                this.createdAt,
                OffsetDateTime.now()
        );
    }


    public static Company construct(
            Long id,
            String companyName,
            String vat,
            String address,
            OffsetDateTime createdAt,
            OffsetDateTime updatedAt
    ) {
        return new Company(
                CompanyId.construct(id),
                CompanyName.construct(companyName),
                Vat.construct(vat),
                Address.construct(address),
                createdAt,
                updatedAt
        );
    }

    public CompanyId getId() {
        return id;
    }

    public CompanyName getCompanyName() {
        return companyName;
    }

    public Vat getVat() {
        return vat;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    public Address getAddress() {
        return address;
    }

    public OffsetDateTime getUpdatedAt() {
        return updatedAt;
    }

    @Override
    public String toString() {
        return "Company{" +
                "id=" + id +
                ", companyName='" + companyName + '\'' +
                ", vat='" + vat + '\'' +
                ", address='" + address + '\'' +
                ", createdAt=" + createdAt +
                ", updatedAt=" + updatedAt +
                '}';
    }
}