package com.training.model;


import java.time.OffsetDateTime;

public class Employee {
    private final EmployeeId id;
    private final FullName fullName;
    private final Email email;
    private final CompanyId companyId;
    private final OffsetDateTime createdAt;
    private final OffsetDateTime updatedAt;


    private Employee(
            EmployeeId id,
            FullName fullName,
            Email email,
            CompanyId companyId,
            OffsetDateTime createdAt,
            OffsetDateTime updatedAt
    ) {
        this.id = id;
        this.fullName = fullName;
        this.email = email;
        this.companyId = companyId;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;

    }

    public static Employee create(
            String firstname,
            String lastname,
            String email,
            Long companyId
    ) {
        OffsetDateTime now = OffsetDateTime.now();
        return new Employee(
                EmployeeId.generateId(),
                FullName.create(firstname, lastname),
                Email.create(email),
                CompanyId.create(companyId),
                now,
                now
        );
    }

    public Employee update(String firstname, String lastname){
        return new Employee(
                this.id,
                FullName.create(firstname, lastname),
                this.email,
                this.companyId,
                this.createdAt,
                OffsetDateTime.now()
        );
    }

    public static Employee construct(
            Long id,
            String firstname,
            String lastname,
            String email,
            Long companyId,
            OffsetDateTime createdAt,
            OffsetDateTime updatedAt
    ) {
        return new Employee(
                EmployeeId.construct(id),
                FullName.construct(firstname, lastname),
                Email.construct(email),
                CompanyId.construct(companyId),
                createdAt,
                updatedAt
        );
    }


    public EmployeeId getId() {
        return id;
    }

    public FullName getFullName(){
        return fullName;
    }

    public Email getEmail() {
        return email;
    }

    public OffsetDateTime getCreatedAt() {
        return createdAt;
    }

    public OffsetDateTime getUpdatedAt() {
        return updatedAt;
    }

    public CompanyId getCompanyId() {
        return companyId;
    }

    @Override
    public String toString() {
        return "Employee{" +
                "id=" + id +
                ", firstname='" + fullName.getFirstname() + '\'' +
                ", lastname='" + fullName.getLastname() + '\'' +
                ", email='" + email + '\'' +
                ", companyId=" + companyId +
                ", createdAt=" + createdAt +
                ", updatedAt=" + updatedAt +
                '}';
    }
}