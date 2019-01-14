package top.dteam.earth.clock.job

import io.reactiverse.pgclient.PgClient
import io.reactiverse.pgclient.PgPool
import spock.lang.Shared
import spock.lang.Specification
import top.dteam.earth.clock.config.ClockConfiguration

class JobHandlerSpec extends Specification{

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

    void "应该能正确执行不需要回调通知的任务"() {

    }

    void "应该能正确执行需要回调通知的任务"() {

    }

    void "未到重试次数的Job应该可以重试"() {

    }

    void "到重试次数的Job应该标记失败"() {

    }

    void "执行回调时未到重试限制失败后会重试"() {

    }

    void "执行回调时到重试限制失败后会标记失败"() {

    }

}
