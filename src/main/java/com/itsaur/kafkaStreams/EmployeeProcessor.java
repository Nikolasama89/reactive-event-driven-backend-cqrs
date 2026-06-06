package com.itsaur.kafkaStreams;

import com.itsaur.event.DomainEvent;
import com.itsaur.event.EmployeeCreatedEvent;
import com.itsaur.event.EmployeeDeletedEvent;
import com.itsaur.event.EmployeeUpdatedEvent;
import com.itsaur.model.Company;
import com.itsaur.model.Employee;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

public class EmployeeProcessor implements Processor<String, DomainEvent, Void, Void> {

    KeyValueStore<String, Employee> employeesStore;
    KeyValueStore<String, String> emailEmployeesStore;

    @Override
    public void init(ProcessorContext<Void, Void> context) {
        this.employeesStore = context.getStateStore(EmployeeTopologyBuilder.EMPLOYEES_STORE);
        this.emailEmployeesStore = context.getStateStore(EmployeeTopologyBuilder.EMAIL_EMPLOYEES_STORE);
    }

    @Override
    public void process(Record<String, DomainEvent> record) {
        String employeeId = record.key();
        DomainEvent event = record.value();

        if (employeeId == null || event == null) return;

        Employee currentEmployee = employeesStore.get(employeeId);
        Employee updatedEmployee = applyEvent(currentEmployee, event);

        if (updatedEmployee == null){
            employeesStore.delete(employeeId);
        } else {
            employeesStore.put(employeeId, updatedEmployee);
        }

        if (event instanceof EmployeeCreatedEvent e){
            emailEmployeesStore.put(e.getEmail().getValue(), employeeId);
        } else if (event instanceof EmployeeDeletedEvent){
            emailEmployeesStore.delete(currentEmployee.getEmail().getValue());
        }

    }

    private Employee applyEvent(Employee current, DomainEvent event) {
        if (event instanceof EmployeeCreatedEvent e) {
            if (current != null) {
                return current;
            }
            return Employee.construct(
                    e.getEmployeeId().getValue(),
                    e.getFullName().getFirstname(),
                    e.getFullName().getLastname(),
                    e.getEmail().getValue(),
                    e.getCompanyId().getValue(),
                    e.getOccurredAt(),
                    e.getOccurredAt()
            );
        }
        if (event instanceof EmployeeUpdatedEvent e) {

            if (current == null) {
                return null;
            }
            return current.update(
                    e.getFullName().getFirstname(),
                    e.getFullName().getLastname()
            );
        }
        if (event instanceof EmployeeDeletedEvent) {
            return null;
        }
        return current;
    }

    @Override
    public void close() {
        Processor.super.close();
    }
}
