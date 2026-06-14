package com.itsaur.config;

import com.itsaur.event.DomainEvent;
import com.itsaur.event.DomainEventListener;
import com.itsaur.event.inMemory.*;
import com.itsaur.event.kafka.KafkaDomainEventConsumer;
import com.itsaur.event.kafka.KafkaDomainEventPublisher;
import com.itsaur.event.logging.*;
import com.itsaur.inMemory.repository.InMemoryCompanyQueryRepository;
import com.itsaur.inMemory.repository.InMemoryCompanyRepository;
import com.itsaur.inMemory.repository.InMemoryEmployeeQueryRepository;
import com.itsaur.inMemory.repository.InMemoryEmployeeRepository;
import com.itsaur.inMemory.search.InMemoryCompanySearch;
import com.itsaur.inMemory.search.InMemoryEmployeeSearch;
import com.itsaur.inMemory.search.InMemoryStorage;
import com.itsaur.model.Company;
import com.itsaur.serializer.DomainEventSerializer;
import com.itsaur.serializer.Serializer;
import com.itsaur.service.CompanyQueryServiceImpl;
import com.itsaur.service.CompanyServiceImpl;
import com.itsaur.service.EmployeeQueryServiceImpl;
import com.itsaur.service.EmployeeServiceImpl;
import com.itsaur.util.KafkaConfig;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.sender.KafkaSender;

import java.util.List;

public class InMemoryKafkaInitializer implements AppEnvInitializer{
    @Override
    public AppContext initialize() throws Exception {

        InMemoryStorage storage = new InMemoryStorage();
        InMemoryEmployeeSearch employeeSearch = new InMemoryEmployeeSearch(storage);
        InMemoryCompanySearch companySearch = new InMemoryCompanySearch(storage);
        String bootstrapServers = "localhost:9092";
        String topic = "domain-events";
        var serializerMap = DomainEventSerdes.serializers();
        var deserializerMap = DomainEventSerdes.deserializers();
        var employeeRepository = new InMemoryEmployeeRepository(storage);
        var employeeQueryRepository = new InMemoryEmployeeQueryRepository(storage, employeeSearch);
        var companyRepository = new InMemoryCompanyRepository(storage);
        var companyQueryRepository = new InMemoryCompanyQueryRepository(storage, companySearch);
        KafkaSender<String, byte[]> sender = KafkaConfig.createProducer(bootstrapServers);
        for (Company company: storage.companies.values()){
            System.out.println(company);
        }

        DomainEventListener<?> companyCreatedLoggingEventListener = new CompanyCreatedLoggingEventListener();
        DomainEventListener<?> companyUpdatedLoggingListener = new CompanyUpdatedLoggingEventListener();
        DomainEventListener<?> companyDeletedLoggingEventListener = new CompanyDeletedLoggingEventListener();
        DomainEventListener<?> employeeCreatedLoggingEventListener = new EmployeeCreatedLoggingEventListener();
        DomainEventListener<?> employeeUpdatedLoggingEventListener = new EmployeeUpdatedLoggingEventListener();
        DomainEventListener<?> employeeDeletedLoggingEventListener = new EmployeeDeletedLoggingEventListener();
        DomainEventListener<?> inMemoryCompanyCreatedEventListener = new InMemoryCompanyCreatedEventListener(companyRepository);
        DomainEventListener<?> inMemoryCompanyUpdatedEventListener = new InMemoryCompanyUpdatedEventListener(companyRepository);
        DomainEventListener<?> inMemoryCompanyDeletedEventListener = new InMemoryCompanyDeletedEventListener(companyRepository);
        DomainEventListener<?> inMemoryEmployeeCreatedEventListener = new InMemoryEmployeeCreatedEventListener(employeeRepository);
        DomainEventListener<?> inMemoryEmployeeUpdatedEventListener = new InMemoryEmployeeUpdatedEventListener(employeeRepository);
        DomainEventListener<?> inMemoryEmployeeDeletedEventListener = new InMemoryEmployeeDeletedEventListener(employeeRepository);

        List<DomainEventListener<?>> listeners = List.of(
                inMemoryCompanyCreatedEventListener,
                companyCreatedLoggingEventListener,
                inMemoryCompanyUpdatedEventListener,
                companyUpdatedLoggingListener,
                inMemoryCompanyDeletedEventListener,
                companyDeletedLoggingEventListener,
                inMemoryEmployeeCreatedEventListener,
                employeeCreatedLoggingEventListener,
                inMemoryEmployeeUpdatedEventListener,
                employeeUpdatedLoggingEventListener,
                inMemoryEmployeeDeletedEventListener,
                employeeDeletedLoggingEventListener
        );

        KafkaReceiver<String, byte[]> inMemoryReceiver =
                KafkaConfig.createReceiver(bootstrapServers, topic, "in-memory-consumer");
        Serializer<DomainEvent> domainSerializer = new DomainEventSerializer(serializerMap);
        KafkaDomainEventConsumer inMemoryConsumer = new KafkaDomainEventConsumer(inMemoryReceiver, listeners, deserializerMap);
        inMemoryConsumer.startConsuming();
        var eventPublisher = new KafkaDomainEventPublisher(sender, topic, domainSerializer);
        var employeeService = new EmployeeServiceImpl(employeeRepository, companyRepository, eventPublisher);
        var companyService = new CompanyServiceImpl(companyRepository, eventPublisher);
        var employeeQueryService = new EmployeeQueryServiceImpl(employeeQueryRepository);
        var companyQueryService = new CompanyQueryServiceImpl(companyQueryRepository);
        return new AppContext(
                employeeService,
                companyService,
                employeeQueryService,
                companyQueryService
        );
    }
}
