# AppDynamics WebFlux Instrumentation

## Use Case
Enables tracking and correlation of HTTP Requests to and from WebFlux/Netty components using the AppDynamics iSDK. 

### NOTE: 

- Version 4.5.2 of the Java agent is required for this to work, the newer java agents are backwards compatible with older controllers (back to 4.4.1) so you should be ok upgrading the agent if your controller is 4.4.1 or newer.

- The jar was built using Java 1.8, feel free to rebuild it using an older java version if required.

## Installation

1. Copy https://github.com/appdynamicsdh/appdynamics-webflux-instrumentation/blob/master/target/AsynciSDK-1.1-SNAPSHOT.jar to /opt/appdynamics/javaagent/verx.x.x.x/sdk-plugins
2. Copy https://github.com/appdynamicsdh/appdynamics-webflux-instrumentation/blob/master/custom-activity-correlation.xml to /opt/appdynamics/javaagent/verx.x.x.x/conf to enable tracking of react threads.
3. Change app-agent-config.xml in the following way:

- **Remove** the following lines from "bci-processing-excludes":

	```
	<custom-exclude filter-type="EQUALS" filter-value="io/netty/util/concurrent/DefaultThreadFactory$DefaultRunnableDecorator"/>
	<custom-exclude filter-type="STARTSWITH" filter-value="io/netty/util/concurrent/SingleThreadEventExecutor"/>
	<custom-exclude filter-type="STARTSWITH" filter-value="io/reactivex/netty/client/ConnectionPoolImpl"/>
	```
- Under:
        ```<include filter-type="STARTSWITH" filter-value="com.ibm.io.async"/>```
- Add:

	```
	<include filter-type="STARTSWITH" filter-value="io.netty.util.concurrent.SingleThreadEventExecutor"/>
	<include filter-type="EQUALS" filter-value="reactor.ipc.netty.tcp.TcpClient$ActiveChannelOperationFactory"/>
	<include filter-type="EQUALS" filter-value="io.netty.util.concurrent.AbstractEventExecutor"/>
	<include filter-type="STARTSWITH" filter-value="io.netty.util.concurrent.DefaultPromise"/>
	<include filter-type="STARTSWITH" filter-value="io.netty.channel.pool.SimpleChannelPool"/>
	<include filter-type="STARTSWITH" filter-value="reactor.ipc.netty.channel.PooledClientContextHandler"/>
	<include filter-type="STARTSWITH" filter-value="io.netty.channel.nio.NioEventLoop"/>
	```

4. Add -Dallow.unsigned.sdk.extension.jars=true to the java agent command line.
5. Restart the Java Agent process.
6. Watch as you see the correlation in the AppDynamics controller (Can take up to 5 minutes).

![Screenshot](https://github.com/appdynamicsdh/appdynamics-webflux-instrumentation/blob/master/webflux.png)

7. Set up a new Async Demarcation point. In the example I was using I had to set up an Async Demarcation point to track the end to end response time for the BT (not the exit call, that's what the plugin does). I did that on "reactor.ipc.netty.http.server.HttpServerOperations/preSendHeadersAndStatus". There may be a better place to do this.

The results of doing that are below:

![Screenshot](https://github.com/appdynamicsdh/appdynamics-webflux-instrumentation/blob/master/webflux2.png)

![Screenshot](https://github.com/appdynamicsdh/appdynamics-webflux-instrumentation/blob/master/webflux4.PNG)

8. It's possible you may see a warning in the logs about ETELatencyCorrelationDelegator-btidVsTimestampMap-limit being exhausted, this can be increased by using setting a node property "ETELatencyCorrelationDelegator-btidVsTimestampMap-limit" to 2000.

## To build

To build this you will need to add the AppDynamics jars to your maven repo:

e.g.

mvn install:install-file -Dfile=C:\appdynamics\AppServerAgent-4.5.2.24246\jackson-annotations-2.8.11.jar -DgroupId=com.appdynamics -DartifactId=jackson-annotations -Dversion=4.5.2 -Dpackaging=jar

mvn install:install-file -Dfile=C:\appdynamics\AppServerAgent-4.5.2.24246\jackson-core-2.8.11.jar -DgroupId=com.appdynamics -DartifactId=jackson-core -Dversion=4.5.2 -Dpackaging=jar

mvn install:install-file -Dfile=C:\appdynamics\AppServerAgent-4.5.2.24246\jackson-databind-2.8.11.1.jar -DgroupId=com.appdynamics -DartifactId=jackson-databind -Dversion=4.5.2 -Dpackaging=jar

mvn install:install-file -Dfile=C:\appdynamics\AppServerAgent-4.5.2.24246\javaagent.jar -DgroupId=com.appdynamics -DartifactId=javaagent -Dversion=4.5.2 -Dpackaging=jar

mvn install:install-file -Dfile=C:\appdynamics\AppServerAgent-4.5.2.24246\uaruleanalyzer.jar -DgroupId=com.appdynamics -DartifactId=uaruleanalyzer -Dversion=4.5.2 -Dpackaging=jar

mvn install:install-file -Dfile=C:\appdynamics\AppServerAgent-4.5.2.24246\ver4.5.2.24246\lib\appagent.jar -DgroupId=com.appdynamics -DartifactId=appagent -Dversion=4.5.2 -Dpackaging=jar

mvn install:install-file -Dfile=C:\appdynamics\AppServerAgent-4.5.2.24246\ver4.5.2.24246\lib\appagent-boot.jar -DgroupId=com.appdynamics -DartifactId=appagent-boot -Dversion=4.5.2 -Dpackaging=jar

mvn install:install-file -Dfile=C:\appdynamics\AppServerAgent-4.5.2.24246\ver4.5.2.24246\lib\common-utils.jar -DgroupId=com.appdynamics -DartifactId=common-utils -Dversion=4.5.2 -Dpackaging=jar

mvn install:install-file -Dfile=C:\appdynamics\AppServerAgent-4.5.2.24246\ver4.5.2.24246\lib\singularity-log4j.jar -DgroupId=com.appdynamics -DartifactId=singularity-log4j -Dversion=4.5.2 -Dpackaging=jar

Then simply run "mvn clean install"


