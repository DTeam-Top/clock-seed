package top.dteam.earth.clock

import groovy.transform.CompileStatic

@CompileStatic
class NamedQuery {

    /**
     * 高优先级、创建时间最长的未处理任务
     * @param limit
     * @return
     */
    static String unprocessedJob(int limit) {
        "select id, topic, body, retry from myjob where status = 'CREATED' order by priority desc, date_created asc limit ${limit}"
    }

    /**
     * 将处理中且未到最大重试次数的任务重置为CREATED
     * @return
     */
    static String resetUnfinishedJob() {
        "update myjob set status = 'CREATED' where status = 'PROCESSING' and retry <= 3"
    }

    /**
     * 设置任务处理标志位
     * @return
     */
    static String setJobProcessing() {
        "update myjob set status = 'PROCESSING' where id = \$1"
    }

    /**
     * 完成任务，将结果（成功：结果，失败：原因）、状态、最后更新时间写入
     * @return
     */
    static String completeJob() {
        '''
          update myjob
          set result = $1
            , status = $2
            , last_updated = now()
          where id = $3
        '''
    }

    /**
     * 记录重试次数并复位任务状态位
     * @return
     */
    static String retryJobNextTime() {
        '''
          update myjob
          set status = 'CREATED'
            , retry = retry + 1
            , last_updated = now()
          where id = $1
        '''
    }

    /**
     * 插入回调任务
     * @return
     */
    static String insertCallbackJob() {
        '''
          insert into myjob (topic, priority, body, status, retry, date_created, last_updated)
            values ('CALLBACK', 10, $1, 'CREATED', 0, now(), now())
        '''
    }

}
