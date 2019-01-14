package top.dteam.earth.clock.job

import io.reactiverse.pgclient.PgClient
import io.reactiverse.pgclient.PgPool
import spock.lang.Shared
import spock.lang.Specification
import top.dteam.earth.clock.config.ClockConfiguration

class JobSchedulerVerticleSpec extends Specification {

    @Shared
    PgPool pgPool

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
        """
        ClockConfiguration clockConfiguration = ClockConfiguration.build(config)
        pgPool = PgClient.pool(clockConfiguration.pgPool())
    }

    void cleanupSpec() {
        pgPool.close()
    }

    void "应该能触发待执行的任务"() {

    }

    void "应该能回收未超过重试限制的僵尸任务且成功触发"() {

    }

    void "无法回收超过重试限制的僵尸任务"() {

    }

    void "回收过程不能干扰正在执行中的任务"() {}

    void "若队列中还有任务，则等待Topic中最小delay值的两倍时间"() {

    }

    void "若队列中没有任务但执行任务中有回调通知，则等待Topic中最小delay值的两倍时间"() {

    }

    void "若队列中没有任务且执行任务也没有回调通知，则等待一段较长时间"() {

    }

}
