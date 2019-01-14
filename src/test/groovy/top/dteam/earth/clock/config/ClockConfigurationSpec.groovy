package top.dteam.earth.clock.config

import spock.lang.Specification
import spock.lang.Unroll

class ClockConfigurationSpec extends Specification {

    def "配置应该可以被正常解析"() {
        setup:
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
            retry = 4
            delay = 600
            limit = 200
            timeout = 8
            callbackRoot = 'http://localhost:8081'
            topics {
                topic1 {
                    key1 = 1000
                }
                topic2 {
                    key2 = 'string'
                }
            }
        """

        when:
        ClockConfiguration clockConfiguration = ClockConfiguration.build(config)

        then:
        with(clockConfiguration.pgPool) {
            host == '127.0.0.1'
            port == 5432
            database == 'earth'
            user == 'earth_admin'
            password == 'admin'
            maxSize == 5
            cachePreparedStatements
        }
        clockConfiguration.retry == 4
        clockConfiguration.delay == 600
        clockConfiguration.limit == 200
        clockConfiguration.timeout == 8
        clockConfiguration.callbackRoot == 'http://localhost:8081'
        clockConfiguration.topics == [topic1: [key1: 1000], topic2: [key2: 'string']]
    }

    def "配置必须包含pgPool测试"() {
        setup:
        String config = ''

        when:
        ClockConfiguration.build(config)

        then:
        thrown(InvalidConfiguriationException)
    }

    def "配置项的默认值测试：delay(500)，retry(3)，limit(100)，timeout(4)，callbackRoot(http://localhost:8080)"() {
        setup:
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

        when:
        ClockConfiguration clockConfiguration = ClockConfiguration.build(config)

        then:
        clockConfiguration.delay == 500
        clockConfiguration.retry == 3
        clockConfiguration.limit == 100
        clockConfiguration.timeout == 4
        clockConfiguration.callbackRoot == 'http://localhost:8080'
    }

    def "对于retry、delay，topic中的同名配置优先。"() {
        setup:
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
            retry = 4
            delay = 600
            limit = 200
            callbackRoot = 'http://localhost:8081'
            topics {
                topic1 {
                    delay = 1000
                    retry = 10
                }
            }
        """

        when:
        ClockConfiguration clockConfiguration = ClockConfiguration.build(config)

        then:
        clockConfiguration.retryByTopic('topic1') == 10
        clockConfiguration.delayByTopic('topic1') == 1000
    }

    def "对于retry、delay，若topic中没有对应配置，则采用全局配置。"() {
        setup:
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
            retry = 4
            delay = 600
            limit = 200
            callbackRoot = 'http://localhost:8081'
            topics {
                topic1 {
                    retry = 10
                }
            }
        """

        when:
        ClockConfiguration clockConfiguration = ClockConfiguration.build(config)

        then:
        clockConfiguration.retryByTopic('topic1') == 10
        clockConfiguration.delayByTopic('topic1') == 600
    }

    def "应该能够找出最小的topic delay值：没有topic且没有配置delay，则为缺省值"() {
        setup:
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

        when:
        ClockConfiguration clockConfiguration = ClockConfiguration.build(config)

        then:
        clockConfiguration.minDelayByTopic() == clockConfiguration.delay
    }

    def "应该能够找出最小的topic delay值：没有topic，有delay，则为配置值"() {
        setup:
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
            delay = 600
        """

        when:
        ClockConfiguration clockConfiguration = ClockConfiguration.build(config)

        then:
        clockConfiguration.minDelayByTopic() == clockConfiguration.delay
    }

    def "应该能够找出最小的topic delay值：全配置"() {
        setup:
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
            delay = 600
            topics {
                topic1 {
                    delay = 100
                }
                topic2 {
                }
            }
        """

        when:
        ClockConfiguration clockConfiguration = ClockConfiguration.build(config)

        then:
        clockConfiguration.minDelayByTopic() == 100
    }

}
