package com.itsaur.event;

public class EmployeeCreatedEvent implements DomainEvent{

    private final Long employeeId;
    private final Long companyId;
    private final String firstname;
    private final String lastname;

    public EmployeeCreatedEvent(Long employeeId, Long companyId, String firstname, String lastname) {
        this.employeeId = employeeId;
        this.companyId = companyId;
        this.firstname = firstname;
        this.lastname = lastname;
    }

    public Long getEmployeeId() {
        return employeeId;
    }

    public Long getCompanyId() {
        return companyId;
    }

    public String getFirstname() {
        return firstname;
    }

    public String getLastname() {
        return lastname;
    }
}
