package com.training.sql.repository;

import com.training.model.Employee;
import com.training.repository.EmployeeRepository;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Row;
import reactor.core.publisher.Mono;
import java.time.OffsetDateTime;


public class DbEmployeeRepository implements EmployeeRepository {

    private final ConnectionFactory cf;


    public DbEmployeeRepository(ConnectionFactory cf) {
        this.cf = cf;
    }

    @Override
    public Mono<Employee> getEmployeeById(Long id) {
        String sql = "select id,firstname,lastname,email, created_at, company_id, updated_at from employees where id = $1";
        return Mono.usingWhen(
                Mono.from(cf.create()),
                conn -> doFindOne(conn, sql, id), Connection::close);
    }

    private Mono<Employee> doFindOne(Connection conn, String sql, Long id) {

        return Mono.from(conn.createStatement(sql).bind(0, id)
                        .execute()
                )
                .flatMap(result -> Mono.from(result.map((row, meta) -> map(row))));

    }

    public Mono<Employee> createOrUpdateEmployee(Long id, Employee employee) {
        if (id == null) {
            String sql =
                    "INSERT INTO employees (id, firstname, lastname, email, company_id) VALUES ($1, $2, $3, $4, $5)" +
                            " RETURNING id, firstname, lastname, email, company_id, created_at, updated_at";
            return Mono.usingWhen(
                    Mono.from(cf.create()),
                    conn -> Mono.from(
                                    conn.createStatement(sql)
                                            .bind(0, employee.getId().getValue())
                                            .bind(1, employee.getFullName().getFirstname())
                                            .bind(2, employee.getFullName().getLastname())
                                            .bind(3, employee.getEmail().getValue())
                                            .bind(4, employee.getCompanyId().getValue())
                                            .execute())
                            .flatMap(result -> Mono.from(result.map((row, meta) -> map(row)))),
                    Connection::close
            );
        } else {
            String sql = "UPDATE employees SET " +
                    "firstname = $1, lastname = $2, email = $3, company_id = $4, updated_at = NOW() WHERE " +
                    "id = $5 RETURNING id, firstname, lastname, email, company_id, created_at, updated_at";
            return Mono.usingWhen(Mono.from(cf.create()), conn -> Mono.from(conn.createStatement(sql)
                                    .bind(0, employee.getFullName().getFirstname())
                                    .bind(1, employee.getFullName().getLastname())
                                    .bind(2, employee.getEmail().getValue())
                                    .bind(3, employee.getCompanyId().getValue())
                                    .bind(4, id)
                                    .execute())
                            .flatMap(result -> Mono.from(result.map((row, meta) -> map(row)))
                            ),
                    Connection::close
            );
        }
    }

    public Mono<Void> deleteEmployee(Long id) {
        String sql = "delete from employees where id = $1";
        return Mono.usingWhen(cf.create(), conn -> Mono.from(conn.createStatement(sql)
                                .bind(0, id)
                                .execute()
                        ).flatMap(result -> Mono.from(result.getRowsUpdated()))
                        .map(rows -> rows != null && rows > 0)
                        .then(), Connection::close
        );
    }


    @Override
    public Mono<Boolean> existsEmail(String email) {
        String sql = "SELECT email FROM employees WHERE email = $1";
        return Mono.usingWhen(cf.create(), conn ->
                        Mono.from(conn.createStatement(sql).bind(0, email).execute()).flatMap(
                                result -> Mono.from(result.map((row, meta) -> row.get(0, String.class)))
                                        .hasElement()),
                Connection::close);
    }

    private Employee map(Row row) {
        return Employee.construct(
                row.get("id", Long.class),
                row.get("firstname", String.class),
                row.get("lastname", String.class),
                row.get("email", String.class),
                row.get("company_id", Long.class),
                row.get("created_at", OffsetDateTime.class),
                row.get("updated_at", OffsetDateTime.class));
    }
}