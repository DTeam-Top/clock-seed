package top.dteam.earth.clock.job

import io.reactiverse.pgclient.PgClient
import io.reactiverse.pgclient.PgPool
import io.reactiverse.pgclient.Row
import io.reactiverse.pgclient.Tuple
import io.reactiverse.pgclient.data.Json
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import spock.lang.Shared
import spock.lang.Specification
import spock.util.concurrent.BlockingVariable
import top.dteam.earth.clock.config.ClockConfiguration
import top.dteam.earth.clock.utils.PgUtils

import java.time.LocalDateTime

class JobSchedulerVerticleSpec extends Specification {

    @Shared
    PgPool pgPool

    @Shared
    Vertx vertx

    void setupSpec() {
        String config = """
            pgPool {
                host = System.getenv("JDBC_HOST") ?: '127.0.0.1'
                port = System.getenv("JDBC_PORT") ? System.getenv("JDBC_PORT") as int : 5432
                database = System.getenv("JDBC_DATABASE") ?: 'earth'
                user = System.getenv("JDBC_USER") ?: 'earth_admin'
                password = System.getenv("JDBC_PASSWORD") ?: 'admin'
                maxSize = 5
                cachePreparedStatements = true
            }
            long delay = 100
        """

        vertx = Vertx.vertx()
        ClockConfiguration.instance = ClockConfiguration.build(config)
        pgPool = PgClient.pool(ClockConfiguration.instance.pgPool())
    }

    void cleanupSpec() {
        pgPool.close()
        vertx.close()
    }

    void "应该能触发待执行的任务"() {
        setup:
        PgUtils pgUtils = new PgUtils(pgPool)
        pgUtils.simpleSql('drop table if exists myjob cascade;') {
            pgUtils.simpleSql('''
                    create table myjob (id bigserial not null
                    , topic varchar(10) not null
                    , priority smallint not null
                    , body jsonb not null
                    , status varchar(10) not null
                    , result jsonb
                    , retry smallint not null
                    , date_created timestamp not null
                    , last_updated timestamp not null
                    , primary key (id));
                ''') {
                pgPool.preparedQuery(''' insert into myjob
                    (topic, priority, body, status, retry, date_created, last_updated)
                    values($1, $2, $3, $4, $5, $6, $7)'''.stripIndent()
                        , Tuple.of('JOB', 5, Json.create(new JsonObject()), 'CREATED', 0, LocalDateTime.now(), LocalDateTime.now())
                ) { ar -> println ar.cause() }
            }
        }
        boolean called = false
        ClockConfiguration.instance.registryHandler('JOB', new JobHandler() {
            @Override
            void handle(Row row) {
                called = true
            }
        })
        JobSchedulerVerticle.DELAY = 100
        JobSchedulerVerticle.SLEEP_FOR_A_WHILE = 100
        BlockingVariable<Row> row = new BlockingVariable<>()

        when:
        sleep(300)
        vertx.deployVerticle(new JobSchedulerVerticle())
        sleep(1000)
        pgUtils.simpleSql('select * from myjob where id = 1') { rowSet ->
            row.set(rowSet.asList()[0])
        }

        then:
        called
        row.get().getString('status') == 'PROCESSING'
    }

    void "应该能重置未超过重试限制的僵尸任务且成功触发"() {
        setup:
        PgUtils pgUtils = new PgUtils(pgPool)
        pgUtils.simpleSql('drop table if exists myjob cascade;') {
            pgUtils.simpleSql('''
                    create table myjob (id bigserial not null
                    , topic varchar(10) not null
                    , priority smallint not null
                    , body jsonb not null
                    , status varchar(10) not null
                    , result jsonb
                    , retry smallint not null
                    , date_created timestamp not null
                    , last_updated timestamp not null
                    , primary key (id));
                ''') {
                pgPool.preparedQuery(''' insert into myjob
                    (topic, priority, body, status, retry, date_created, last_updated)
                    values($1, $2, $3, $4, $5, $6, $7)'''.stripIndent()
                        , Tuple.of('JOB', 5, Json.create(new JsonObject()), 'PROCESSING', 0, LocalDateTime.now().minusHours(10), LocalDateTime.now().minusHours(10))
                ) { ar -> println ar.cause() }
            }
        }
        boolean called = false
        ClockConfiguration.instance.registryHandler('JOB', new JobHandler() {
            @Override
            void handle(Row row) {
                called = true
            }
        })
        JobSchedulerVerticle.DELAY = 100
        JobSchedulerVerticle.SLEEP_FOR_A_WHILE = 100
        JobSchedulerVerticle.INTERVAL = 100
        BlockingVariable<Row> row = new BlockingVariable<>()

        when:
        sleep(300)
        vertx.deployVerticle(new JobSchedulerVerticle())
        sleep(1000)
        pgUtils.simpleSql('select * from myjob where id = 1') { rowSet ->
            row.set(rowSet.asList()[0])
        }

        then:
        called
        row.get().getString('status') == 'PROCESSING'
        row.get().getInteger('retry') == 1
        row.get().getLocalDateTime('last_updated').plusMinutes(10) > LocalDateTime.now()
    }

    void "无法重置超过重试限制的僵尸任务"() {
        setup:
        PgUtils pgUtils = new PgUtils(pgPool)
        pgUtils.simpleSql('drop table if exists myjob cascade;') {
            pgUtils.simpleSql('''
                    create table myjob (id bigserial not null
                    , topic varchar(10) not null
                    , priority smallint not null
                    , body jsonb not null
                    , status varchar(10) not null
                    , result jsonb
                    , retry smallint not null
                    , date_created timestamp not null
                    , last_updated timestamp not null
                    , primary key (id));
                ''') {
                pgPool.preparedQuery(''' insert into myjob
                    (topic, priority, body, status, retry, date_created, last_updated)
                    values($1, $2, $3, $4, $5, $6, $7)'''.stripIndent()
                        , Tuple.of('JOB', 5, Json.create(new JsonObject()), 'PROCESSING', 3, LocalDateTime.now().minusHours(10), LocalDateTime.now().minusHours(10))
                ) { ar -> println ar.cause() }
            }
        }
        boolean called = false
        ClockConfiguration.instance.registryHandler('JOB', new JobHandler() {
            @Override
            void handle(Row row) {
                called = true
            }
        })
        JobSchedulerVerticle.DELAY = 100
        JobSchedulerVerticle.SLEEP_FOR_A_WHILE = 100
        JobSchedulerVerticle.INTERVAL = 100
        BlockingVariable<Row> row = new BlockingVariable<>()

        when:
        sleep(300)
        vertx.deployVerticle(new JobSchedulerVerticle())
        sleep(1000)
        pgUtils.simpleSql('select * from myjob where id = 1') { rowSet ->
            row.set(rowSet.asList()[0])
        }

        then:
        !called
        row.get().getString('status') == 'PROCESSING'
        row.get().getInteger('retry') == 3
        row.get().getLocalDateTime('last_updated').plusMinutes(10) < LocalDateTime.now()
    }

    void "重置过程不能干扰正在执行中的任务"() {
        setup:
        PgUtils pgUtils = new PgUtils(pgPool)
        pgUtils.simpleSql('drop table if exists myjob cascade;') {
            pgUtils.simpleSql('''
                    create table myjob (id bigserial not null
                    , topic varchar(10) not null
                    , priority smallint not null
                    , body jsonb not null
                    , status varchar(10) not null
                    , result jsonb
                    , retry smallint not null
                    , date_created timestamp not null
                    , last_updated timestamp not null
                    , primary key (id));
                ''') {
                pgPool.preparedBatch(''' insert into myjob
                    (topic, priority, body, status, retry, date_created, last_updated)
                    values($1, $2, $3, $4, $5, $6, $7)'''.stripIndent(), [
                        Tuple.of('JOB', 5, Json.create(new JsonObject()), 'PROCESSING', 0, LocalDateTime.now().minusHours(10), LocalDateTime.now().minusHours(10))
                        , Tuple.of('JOB', 5, Json.create(new JsonObject()), 'PROCESSING', 0, LocalDateTime.now(), LocalDateTime.now())
                ]) { ar -> println ar.cause() }
            }
        }
        boolean called = false
        ClockConfiguration.instance.registryHandler('JOB', new JobHandler() {
            @Override
            void handle(Row row) {
                called = true
            }
        })
        JobSchedulerVerticle.DELAY = 100
        JobSchedulerVerticle.SLEEP_FOR_A_WHILE = 100
        JobSchedulerVerticle.INTERVAL = 100
        BlockingVariable<List<Row>> rows = new BlockingVariable<>()

        when:
        sleep(300)
        vertx.deployVerticle(new JobSchedulerVerticle())
        sleep(1000)
        pgUtils.simpleSql('select * from myjob order by id') { rowSet ->
            rows.set(rowSet.asList())
        }

        then:
        called
        rows.get()[0].getInteger('retry') == 1
        rows.get()[1].getInteger('retry') == 0
    }

}
