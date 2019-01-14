package top.dteam.earth.clock;

import io.vertx.core.AbstractVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import top.dteam.earth.clock.job.JobSchedulerVerticle;

public class MainVerticle extends AbstractVerticle {

    private final static Logger logger = LoggerFactory.getLogger(MainVerticle.class);

    @Override
    public void start() {
        deploySubVerticles();
    }

    private void deploySubVerticles() {
        vertx.deployVerticle(new JobSchedulerVerticle(), ar -> {
            if (ar.succeeded()) {
                logger.info("Job Scheduler Verticle started ...");
            }
        });
    }

}
