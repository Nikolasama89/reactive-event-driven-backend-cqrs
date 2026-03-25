package com.itsaur.event;

public class EmployeeDeletedEvent implements DomainEvent{

    private final Long employeeId;
    private final Long companyId;

    public EmployeeDeletedEvent(Long employeeId, Long companyId) {
        this.employeeId = employeeId;
        this.companyId = companyId;
    }

    public Long getEmployeeId() {
        return employeeId;
    }

    public Long getCompanyId() {
        return companyId;
    }
}
