# Clock Seed

Clock模板工程为基于Vert.x和数据库的简单任务队列提供了基础。它目前支持的任务类型（Topic）：
- topic = ‘SMS’，短消息发送
- topic = ‘CALLBACK’，回调任务

## 任务相关

Clock可以跟[Grails REST SEED](https://github.com/DTeam-Top/grails-rest-seed)工程模板配套使用，后者为前者提供了任务来源。对于每个任务，其属性如下：
- topic，任务主题，即分类
- priority，优先级，缺省为5，最高为10，最低为1。优先级高的任务优先执行。
- body，任务参数，类型为jsonb
- status，任务状态
- result，执行结果，类型为jsonb
- retry，重试次数
- date_created，任务创建时间
- last_updated，最后更新时间

任务的产生就是简单地生成上述任务记录，任务记录保存于myjob表中，其结构如下：

~~~
create table myjob (
    id bigserial not null
    , topic varchar(10) not null
    , priority smallint not null
    , body jsonb not null
    , status varchar(10) not null
    , result jsonb
    , retry smallint not null
    , date_created timestamp not null
    , last_updated timestamp not null
    , primary key (id));
~~~

对于每个任务，其缺省的重试次数为3次，因此其总的执行次数为4次：1次执行 + 3次重试。

### 回调任务

除topic为‘CALLBACK’的任务外，所有任务都由外部使用者创建。假如任务对应的body包含有callback属性，同时它又不是回调任务，则在它执行完毕之后，将会同时创建回调任务，并且回调任务的body结构如下：
- source，对应的源任务id
- callback，回调地址，取自源任务body的callback属性
- result，源任务的结果

通过这样的机制，将回调也转换成普通的任务对待。

### 任务状态

对于每个任务，其状态如下：
- CREATED，初始状态
- PROCESSING，执行状态，在任务执行前，其对应的status会被更改为这个状态
- SUCCEED，成功结束，任务成功执行完后处于这个状态
- FAILED，任务失败

状态变化路径见下面几节。

#### CREATED -> PROCESSING -> SUCCEED

这是最普通的情况，任务顺利执行并结束：
- result记录执行结果。
- 若任务需要回调通知，即body中包含callback，同时会创建对应的回调任务。

#### CREATED -> PROCESSING -> CREATED

这条路径对应两种可能：
1. 任务执行失败，但没有超过重试次数。
1. 任务执行因为某种原因被挂起，成为僵尸任务。这类任务会有另一个重置定时器任务来将任务状态复位。

不论哪种情况，重试次数都会加1。

#### CREATED -> PROCESSING -> FAILED

任务在超出重试次数之后，失败。此时任务的result记录失败原因。

## 使用指南

本工程的基本使用：
- 扩展AbstractJobHandler开发自己的任务
- 编写对应的测试
- 在Launcher中注册任务到对应的topic
- 打包：gradle shadowjar
- 书写配置文件
- 运行：java -jar -Dconf=./conf.tmp build/libs/clock-\<version\>-fat.jar

本工程的配置文件示例如下（Groovy DSL）：
~~~
pgPool {
    host = System.getenv("JDBC_HOST") ?: '127.0.0.1'
    port = System.getenv("JDBC_PORT") ? System.getenv("JDBC_PORT") as int : 5432
    database = System.getenv("JDBC_DATABASE") ?: 'earth'
    user = System.getenv("JDBC_USER") ?: 'earth_admin'
    password = System.getenv("JDBC_PASSWORD") ?: 'admin'
    maxSize = 5
    cachePreparedStatements = true
}

retry = 3
delay = 500
limit = 100
timeout = 8
callbackRoot = 'http://localhost:8080'

topics {

    SMS {
        key = System.getenv("BARN_KEY") ?: 'mock'
        password = System.getenv("BARN_KS_PASSWORD") ?: 'mock'
    }

    CALLBACK {
        delay = 1000
        retry = 10
    }

}
~~~

其中：
- pgPool：数据库配置
- retry：重试次数
- delay：任务延迟执行时间，ms
- limit：一次从数据库中取的最大数据条数
- timeout：僵尸任务时限，hour
- callbackRoot：回调url的root url
- topics：任务特定配置

对于retry和delay，任务特定的配置可以覆盖全局配置。假如任务特定配置中没有，则采用全局配置。

上述配置除了pgPool，其余都是可选的，它们对应的缺省值如下：
- delay：500 ms
- retry：3
- limit：100
- timeout：4 hour
- callbackRoot：http://localhost:8080

## 代码贡献

本项目使用`codenarc`,`pmd`,`spotbugs`检查项目代码规范，相关的规则配置存储于`config/`目录下，提交代码前使用

```bash
gradle codenarcMain pmdMain spotbugsMain
```

执行代码规范检查，并且确保`gradle test`通过测试。

TODO: 找一个好用的git hook插件，可以在提交代码前自动运行代码规范检查
