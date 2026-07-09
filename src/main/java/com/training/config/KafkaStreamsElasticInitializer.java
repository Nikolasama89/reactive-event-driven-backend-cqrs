package com.training.config;

import com.training.elastic.event.*;
import com.training.elastic.repository.ElasticCompanyQueryRepository;
import com.training.elastic.repository.ElasticEmployeeQueryRepository;
import com.training.elastic.search.ElasticSearchCompanyQueryBuilder;
import com.training.elastic.search.ElasticSearchEmployeeQueryBuilder;
import com.training.elastic.service.ElasticCompanyQueryServiceImpl;
import com.training.elastic.service.ElasticEmployeeQueryServiceImpl;
import com.training.event.*;
import com.training.event.kafka.KafkaDomainEventConsumer;
import com.training.event.kafka.KafkaDomainEventPublisher;
import com.training.kafkaStreams.CompanyTopologyBuilder;
import com.training.kafkaStreams.EmployeeTopologyBuilder;
import com.training.kafkaStreams.StreamCompanyRepository;
import com.training.kafkaStreams.StreamEmployeeRepository;
import com.training.model.Company;
import com.training.model.Employee;
import com.training.serializer.*;
import com.training.service.CompanyServiceImpl;
import com.training.service.EmployeeServiceImpl;
import com.training.util.ElasticSearchConfig;
import com.training.util.KafkaConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.sender.KafkaSender;

import java.util.List;
import java.util.Properties;

public class KafkaStreamsElasticInitializer implements AppEnvInitializer{
    @Override
    public AppContext initialize() throws Exception {
        var client = ElasticSearchConfig.getClient();
        var serializerMap = DomainEventSerdes.serializers();
        var deserializerMap = DomainEventSerdes.deserializers();

        String bootstrapServers = "localhost:9092";

        Properties companyProps = new Properties();
        companyProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "company-streams-app");
        companyProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        companyProps.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        companyProps.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        Properties employeeProps = new Properties();
        employeeProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "employee-streams-app");
        employeeProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        employeeProps.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        employeeProps.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);

        Serde<DomainEvent> eventSerde = Serdes.serdeFrom(
                new StreamDomainEventSerializer(), new StreamDomainEventDeserializer());
        Serde<Company> companySerde = Serdes.serdeFrom(
                new StreamCompanySerializer(), new StreamCompanyDeserializer());
        Serde<Employee> employeeSerde = Serdes.serdeFrom(
                new StreamEmployeeSerializer(), new StreamEmployeeDeserializer()
        );

        Topology companyTopology = new CompanyTopologyBuilder(eventSerde, companySerde).build();
        Topology employeeTopology = new EmployeeTopologyBuilder(eventSerde, employeeSerde).build();

        KafkaStreams companyStreams = new KafkaStreams(companyTopology, companyProps);
        KafkaStreams employeeStreams = new KafkaStreams(employeeTopology, employeeProps);
        companyStreams.start();
        employeeStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(companyStreams::close));
        Runtime.getRuntime().addShutdownHook(new Thread(employeeStreams::close));

        DomainEventListener<CompanyCreatedEvent> companyCreatedElasticEventListener =
                new CompanyCreatedElasticEventListener(client);

        DomainEventListener<CompanyUpdatedEvent> companyUpdatedElasticEventListener =
                new CompanyUpdatedElasticEventListener(client);

        DomainEventListener<CompanyDeletedEvent> companyDeletedElasticEventListener =
                new CompanyDeletedElasticEventListener(client);

        DomainEventListener<CompanyUpdatedEvent> employeeAfterCompanyUpdated =
                new EmployeeAfterCompanyUpdatedElasticEventListener(client);

        DomainEventListener<EmployeeCreatedEvent> employeeCreatedElasticEventListener =
                new EmployeeCreatedElasticEventListener(client);

        DomainEventListener<EmployeeUpdatedEvent> employeeUpdatedElasticEventListener =
                new EmployeeUpdatedElasticEventListener(client);

        DomainEventListener<EmployeeDeletedEvent> employeeDeletedElasticEventListener =
                new EmployeeDeletedElasticEventListener(client);

        DomainEventListener<EmployeeCreatedEvent> employeeIncrementCompanyTotalEmployeesListener =
                new EmployeeCreateIncrementTotalEmployeesListener(client);

        DomainEventListener<EmployeeDeletedEvent> employeeDecrementCompanyTotalEmployeesListener =
                new EmployeeDeletedDecrementTotalEmployeesListener(client);

        DomainEventListener<EmployeeCreatedEvent> employeeNameAddToCompanyListener =
                new IndexAddEmployeeNameToCompanyElasticEventListener(client);

        DomainEventListener<EmployeeDeletedEvent> employeeDeleteNameFromCompanyIndexListener =
                new IndexRemoveEmployeeFromCompanyElasticEventListener(client);

        DomainEventListener<EmployeeUpdatedEvent> employeeUpdateNameAtCompanyIndexListener =
                new IndexUpdateEmployeeInCompanyElasticEventListener(client);

        List<DomainEventListener<?>> elasticListeners = List.of(
                companyCreatedElasticEventListener,
                companyUpdatedElasticEventListener,
                employeeUpdateNameAtCompanyIndexListener,
                employeeAfterCompanyUpdated,
                companyDeletedElasticEventListener,
                employeeCreatedElasticEventListener,
                employeeIncrementCompanyTotalEmployeesListener,
                employeeNameAddToCompanyListener,
                employeeUpdatedElasticEventListener,
                employeeDeletedElasticEventListener,
                employeeDecrementCompanyTotalEmployeesListener,
                employeeDeleteNameFromCompanyIndexListener
        );

        String topic = "domain-events";

        KafkaSender<String, byte[]> sender =
                KafkaConfig.createProducer(bootstrapServers);

        KafkaReceiver<String, byte[]> elasticReceiver =
                KafkaConfig.createReceiver(bootstrapServers, topic, "elastic-consumer");

        KafkaDomainEventConsumer elasticConsumer =
                new KafkaDomainEventConsumer(
                        elasticReceiver,
                        elasticListeners,
                        deserializerMap
                );

        elasticConsumer.startConsuming();

        Serializer<DomainEvent> domainSerializer =
                new DomainEventSerializer(serializerMap);

        var employeeRepository = new StreamEmployeeRepository(employeeStreams);
        var companyRepository = new StreamCompanyRepository(companyStreams);
        var companyQueryBuilder = new ElasticSearchCompanyQueryBuilder();
        var employeeQueryBuilder = new ElasticSearchEmployeeQueryBuilder();
        var elasticEmployeeQueryRepository = new ElasticEmployeeQueryRepository(client, employeeQueryBuilder);
        var elasticCompanyQueryRepository = new ElasticCompanyQueryRepository(client, companyQueryBuilder);
        var eventPublisher = new KafkaDomainEventPublisher(sender, topic, domainSerializer);
        var employeeService = new EmployeeServiceImpl(employeeRepository, companyRepository, eventPublisher);
        var companyService = new CompanyServiceImpl(companyRepository, eventPublisher);
        var employeeQueryService = new ElasticEmployeeQueryServiceImpl(elasticEmployeeQueryRepository);
        var companyQueryService = new ElasticCompanyQueryServiceImpl(elasticCompanyQueryRepository);

        System.out.println("Running KAFKA STREAMS with ELASTIC");
        return new AppContext(
                employeeService,
                companyService,
                employeeQueryService,
                companyQueryService
        );
    }
}
