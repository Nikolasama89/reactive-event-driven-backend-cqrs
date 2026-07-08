package com.itsaur.kafkaStreams;

import com.itsaur.model.Employee;
import com.itsaur.repository.EmployeeRepository;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class StreamEmployeeRepository implements EmployeeRepository {
    private final KafkaStreams kafkaStreams;

    public StreamEmployeeRepository(KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }

    @Override
    public Mono<Employee> getEmployeeById(Long id) {
        String employeeId = Long.toString(id);
        return Mono.fromCallable(() -> employeeStore().get(employeeId))
                .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<Boolean> existsEmail(String email) {
        return Mono.fromCallable(() -> employeeEmailStore().get(email) != null)
                .subscribeOn(Schedulers.boundedElastic());
    }

    private ReadOnlyKeyValueStore<String, Employee> employeeStore(){
        return kafkaStreams.store(StoreQueryParameters.fromNameAndType(
                EmployeeTopologyBuilder.EMPLOYEES_STORE, QueryableStoreTypes.keyValueStore()));
    }

    private ReadOnlyKeyValueStore<String, String> employeeEmailStore(){
        return kafkaStreams.store(StoreQueryParameters.fromNameAndType(
                EmployeeTopologyBuilder.EMAIL_EMPLOYEES_STORE, QueryableStoreTypes.keyValueStore()));
    }
}
