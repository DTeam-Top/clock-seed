import ch.qos.logback.classic.encoder.PatternLayoutEncoder

appender("Console", ConsoleAppender) {
    encoder(PatternLayoutEncoder) {
        pattern = "%d [%thread] %-5level %logger{36} - %msg%n"
    }
}

appender("R", RollingFileAppender) {
    file = "clock.log"
    encoder(PatternLayoutEncoder) {
        pattern = "%d [%thread] %-5level %logger{36} - %msg%n"
    }
    rollingPolicy(FixedWindowRollingPolicy) {
        fileNamePattern = "clock.log.%i"
        minIndex = 1
        maxIndex = 10
    }
    triggeringPolicy(SizeBasedTriggeringPolicy) {
        maxFileSize = "10MB"
    }
}

logger("io.vertx", WARN)
logger("io.netty", WARN)
logger("ch.qos.logback", WARN)
logger("io.vertx", WARN)

final String CLOCK_LOG_LEVEL = System.getProperty("CLOCK_LOG_LEVEL") ?: System.getenv("CLOCK_LOG_LEVEL")

root(valueOf(CLOCK_LOG_LEVEL), ["Console", "R"])
