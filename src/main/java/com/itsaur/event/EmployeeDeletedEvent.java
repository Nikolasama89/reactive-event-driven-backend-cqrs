package com.itsaur.event;

import com.itsaur.model.CompanyId;
import com.itsaur.model.EmployeeId;

public class EmployeeDeletedEvent implements DomainEvent{
    private final EmployeeId employeeId;
    private final CompanyId companyId;


    public EmployeeDeletedEvent(EmployeeId employeeId, CompanyId companyId) {
        this.employeeId = employeeId;
        this.companyId = companyId;
    }

    public EmployeeId getEmployeeId() {
        return employeeId;
    }

    public CompanyId getCompanyId() {
        return companyId;
    }

    @Override
    public String aggregateId() {
        return employeeId.toString();
    }
}
