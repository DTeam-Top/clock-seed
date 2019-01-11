package top.dteam.earth.clock.job;

import io.reactiverse.pgclient.Row;

@FunctionalInterface
public interface JobHandler {

    void handle(Row row);

}
