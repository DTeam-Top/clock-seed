package top.dteam.earth.clock.config

import groovy.transform.PackageScope
import io.reactiverse.pgclient.PgPoolOptions
import io.vertx.core.Vertx
import top.dteam.earth.clock.job.JobHandler
import top.dteam.earth.clock.job.handler.CallbackJobHandler
import top.dteam.earth.clock.job.handler.SmsJobHandler

class ClockConfiguration {

    static ClockConfiguration instance

    @PackageScope
    PgPoolOptions pgPool

    @PackageScope
    long delay = 500

    @PackageScope
    int retry = 3

    @PackageScope
    String callbackRoot = 'http://localhost:8080'

    @PackageScope
    int limit = 100

    @PackageScope
    int timeout = 4

    @PackageScope
    Map<String, Map> topics

    @PackageScope
    Map<String, JobHandler> jobHandlers = [:]

    static void load(String file = System.getProperty('conf')) {
        String config
        if (file) {
            config = new File(file).text
        } else {
            throw new FileNotFoundException('请提供配置文件！')
        }

        instance = build(config)
    }

    static ClockConfiguration build(String config) {
        ConfigObject configObject = new ConfigSlurper().parse(config)
        ClockConfiguration configuration = new ClockConfiguration(configObject)

        if (!configuration.pgPool) {
            throw new InvalidConfiguriationException('缺少pgPool配置！')
        }

        configuration
    }

    PgPoolOptions pgPool() {
        pgPool
    }

    int limit() {
        limit
    }

    int timeout() {
        timeout
    }

    String callbackRoot() {
        callbackRoot
    }

    long delayByTopic(String topic) {
        topics[topic]?.delay ?: delay
    }

    int retryByTopic(String topic) {
        topics[topic]?.retry ?: retry
    }

    Map topicConfig(String topic) {
        Collections.unmodifiableMap(topics[topic])
    }

    int minDelayByTopic() {
        Long.min(topics?.values()?.min({ it.delay ?: Long.MAX_VALUE })?.delay ?: Long.MAX_VALUE, delay)
    }

    void registryHandler(String topic, JobHandler jobHandler) {
        jobHandlers[topic] = jobHandler
    }

    JobHandler jobHandlers(String topic) {
        jobHandlers[topic]
    }

}
