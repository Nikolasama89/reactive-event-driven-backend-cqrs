package com.itsaur.event;

public class EmployeeUpdatedEvent implements DomainEvent{
    private final Long employeeId;

    public EmployeeUpdatedEvent(Long employeeId) {
        this.employeeId = employeeId;
    }

    public Long getEmployeeId() {
        return employeeId;
    }
}
