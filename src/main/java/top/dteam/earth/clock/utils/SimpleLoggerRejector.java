package top.dteam.earth.clock.utils;

import io.vertx.core.Handler;
import org.slf4j.Logger;

public class SimpleLoggerRejector implements Handler<Throwable> {

    private transient final Logger logger;

    public SimpleLoggerRejector(Logger logger) {
        this.logger = logger;
    }

    @Override
    public void handle(Throwable throwable) {
        logger.error(throwable.getMessage());
    }
}
