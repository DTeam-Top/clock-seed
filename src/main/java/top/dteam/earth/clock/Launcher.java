package top.dteam.earth.clock;

import top.dteam.earth.clock.config.ClockConfiguration;

public class Launcher extends io.vertx.core.Launcher {

    public static void main(String[] args) {
        System.setProperty("vertx.logger-delegate-factory-class-name",
                "io.vertx.core.logging.SLF4JLogDelegateFactory");

        ClockConfiguration.load();

        new Launcher().dispatch(args);
    }

}
