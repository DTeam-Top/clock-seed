package top.dteam.earth.clock;

import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.dteam.earth.clock.config.ClockConfiguration;
import top.dteam.earth.clock.job.handler.CallbackJobHandler;
import top.dteam.earth.clock.job.handler.SmsJobHandler;

public class Launcher extends io.vertx.core.Launcher {

    private final static Logger logger = LoggerFactory.getLogger(Launcher.class);

    public static void main(String[] args) {
        System.setProperty("vertx.logger-delegate-factory-class-name",
                "io.vertx.core.logging.SLF4JLogDelegateFactory");

        new Launcher().dispatch(args);
    }

    @Override
    public void afterStartingVertx(Vertx vertx) {
        logger.info("Loading configuration and registering job handlers ...");

        ClockConfiguration.load();
        ClockConfiguration configuration = ClockConfiguration.getInstance();
        configuration.registryHandler("SMS", new SmsJobHandler(vertx));
        configuration.registryHandler("CALLBACK", new CallbackJobHandler(vertx));
    }

}
