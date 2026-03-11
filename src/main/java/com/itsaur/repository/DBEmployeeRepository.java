package com.itsaur.repository;

import com.itsaur.model.Employee;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Mono;
import java.time.OffsetDateTime;


public class DBEmployeeRepository implements EmployeeRepository {

    private final ConnectionFactory cf;


    public DBEmployeeRepository(ConnectionFactory cf) {
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
                .flatMap(result -> Mono.from(result.map((row, meta) ->
                                new Employee(
                        row.get("id", Long.class),
                        row.get("firstname", String.class),
                        row.get("lastname", String.class),
                        row.get("email", String.class),
                        row.get("company_id", Long.class),
                        row.get("created_at", OffsetDateTime.class),
                        row.get("updated_at", OffsetDateTime.class)
                    )
                        ))
                );
    }


    @Override
    public Mono<Employee> createOrUpdateEmployee(Long id, Employee employee) {
        if (id == null) {
            String sql = "INSERT INTO employees (firstname, lastname, email, company_id) VALUES ($1, $2, $3, $4) RETURNING id, firstname, lastname, email, company_id";
            return Mono.usingWhen(
                    Mono.from(cf.create()),
                    conn -> doCreateEmployee(conn, sql, employee),
                    Connection::close
            );
        } else {
            String sql = "UPDATE employees SET firstname = $1, lastname = $2, email = $3, company_id = $4, updated_at = NOW() WHERE id = $5 RETURNING id, firstname, lastname, email, company_id, created_at, updated_at";
            return Mono.usingWhen(Mono.from(cf.create()), conn -> Mono.from(conn.createStatement(sql)
                            .bind(0, employee.getFirstname())
                            .bind(1, employee.getLastname())
                            .bind(2, employee.getEmail())
                            .bind(3, employee.getCompanyId())
                            .bind(4, id)
                            .execute()).flatMap(result -> Mono.from(result.map((row, meta) -> {
                                Employee e = new Employee();
                                e.setId(row.get("id", Long.class));
                                e.setFirstname(row.get("firstname", String.class));
                                e.setLastname(row.get("lastname", String.class));
                                e.setEmail(row.get("email", String.class));
                                e.setCompanyId(row.get("company_id", Long.class));
                                e.setCreatedAt(row.get("created_at", OffsetDateTime.class));
                                e.setUpdatedAt(row.get("updated_at", OffsetDateTime.class));
                                return e;
                            }))
                    ),
                    Connection::close
            );
        }
    }

    private Mono<Employee> doCreateEmployee(Connection conn, String sql, Employee employee) {
        return Mono.from(
                conn.createStatement(sql)
                        .bind(0, employee.getFirstname())
                        .bind(1, employee.getLastname())
                        .bind(2, employee.getEmail())
                        .bind(3, employee.getCompanyId())
                        .execute())
                .flatMap(result -> Mono.from(result.map((row, meta) -> {
                    Employee e = new Employee();
                    e.setId(row.get("id", Long.class));
                    e.setFirstname(row.get("firstname", String.class));
                    e.setLastname(row.get("lastname", String.class));
                    e.setEmail(row.get("email", String.class));
                    e.setCompanyId(row.get("company_id", Long.class));
                    return e;
                })));
    }


    @Override
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
}
