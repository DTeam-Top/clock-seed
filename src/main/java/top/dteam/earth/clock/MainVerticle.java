package top.dteam.earth.clock;

import io.vertx.core.AbstractVerticle;
import top.dteam.earth.clock.job.JobHandler;
import top.dteam.earth.clock.job.JobSchedulerVerticle;
import top.dteam.earth.clock.job.handler.CallbackJobHandler;
import top.dteam.earth.clock.job.handler.SmsJobHandler;

import java.util.HashMap;
import java.util.Map;

public class MainVerticle extends AbstractVerticle {

    public static Map<String, JobHandler> jobHandlers;

    @Override
    public void start() {
        initTopicHandlers();
        deploySubVerticles();
    }

    private void initTopicHandlers() {
        jobHandlers = new HashMap<>();
        jobHandlers.put("SMS", new SmsJobHandler(vertx));
        jobHandlers.put("CALLBACK", new CallbackJobHandler(vertx));
    }

    private void deploySubVerticles() {
        vertx.deployVerticle(new JobSchedulerVerticle());
    }

}
