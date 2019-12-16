package com.appdynamics;

import com.appdynamics.agent.api.AppdynamicsAgent;
import com.appdynamics.agent.api.EntryTypes;
import com.appdynamics.agent.api.Transaction;
import com.appdynamics.apm.appagent.api.DataScope;
import com.appdynamics.apm.appagent.api.ITransactionDemarcator;
import com.appdynamics.instrumentation.sdk.Rule;
import com.appdynamics.instrumentation.sdk.SDKClassMatchType;
import com.appdynamics.instrumentation.sdk.SDKStringMatchType;
import com.appdynamics.instrumentation.sdk.contexts.ISDKUserContext;
import com.appdynamics.instrumentation.sdk.template.AEntry;
import com.appdynamics.instrumentation.sdk.template.AGenericInterceptor;
import com.appdynamics.instrumentation.sdk.toolbox.reflection.IReflector;
import com.appdynamics.instrumentation.sdk.toolbox.reflection.ReflectorException;

import java.lang.reflect.Field;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KafkaStreamsGenericInterceptor extends AGenericInterceptor {

    private static final String CLASS_TO_INSTRUMENT = "org.apache.kafka.streams.kstream.Transformer";
    private static final String METHOD_TO_INSTRUMENT = "transform";

    private static final String CLASS_TO_INSTRUMENT_2 = "org.apache.kafka.streams.processor.Processor";
    private static final String METHOD_TO_INSTRUMENT_2 = "process";

    private IReflector key = null;
    private IReflector value = null;
    private IReflector lastHeader = null;
    private static Pattern r = null;

    private IReflector getHeaders = null;
    private boolean identifyBt = true;

    static{
        //appId=580*ctrlguid=1568857184*acctguid=a88c08ac-78c4-4eea-8bbc-2ea492c04489*ts=1572458463462*btid=101217*guid=70d67a8c-8734-4a4d-976a-1bd843d3731b*exitguid=5*unresolvedexitid=5082481*cidfrom=4048*etypeorder=CUSTOM*esubtype=Kafka*cidto={[UNRESOLVED][5082481]}

        String pattern = "ts=(\\d+).*btid=(\\d+).*exitguid=(\\d+).*";
        r = Pattern.compile(pattern);
    }

    @Override
    public List<Rule> initializeRules() {
        List<Rule> result = new ArrayList<>();

        Rule.Builder bldr = new Rule.Builder(CLASS_TO_INSTRUMENT);
        bldr = bldr.classMatchType(SDKClassMatchType.IMPLEMENTS_INTERFACE).classStringMatchType(SDKStringMatchType.EQUALS);
        bldr = bldr.methodMatchString(METHOD_TO_INSTRUMENT).methodStringMatchType(SDKStringMatchType.EQUALS);
        result.add(bldr.build());

        Rule.Builder bldr2 = new Rule.Builder(CLASS_TO_INSTRUMENT_2);
        bldr2 = bldr2.classMatchType(SDKClassMatchType.IMPLEMENTS_INTERFACE).classStringMatchType(SDKStringMatchType.EQUALS);
        bldr2 = bldr2.methodMatchString(METHOD_TO_INSTRUMENT_2).methodStringMatchType(SDKStringMatchType.EQUALS);
        result.add(bldr2.build());



        return result;
    }

    public KafkaStreamsGenericInterceptor() {
        super();
        boolean searchSuperClass = true;


        getHeaders = getNewReflectionBuilder()
                .invokeInstanceMethod("headers", searchSuperClass)
                .build();

        value = getNewReflectionBuilder()
                .invokeInstanceMethod("value", searchSuperClass)
                .build();


        String[] types = new String[]{String.class.getCanonicalName()};
        this.lastHeader = this.getNewReflectionBuilder().invokeInstanceMethod("lastHeader", searchSuperClass, types).build();

    }

    public class KafkaDataHolder{
        Transaction transaction;
        String correlationId;

        public Transaction getTransaction() {
            return transaction;
        }
        public void setTransaction(Transaction transaction) {
            this.transaction = transaction;
        }
        public String getCorrelationId() {
            return correlationId;
        }
        public void setCorrelationId(String correlationId) {
            this.correlationId = correlationId;
        }
    }

    private Boolean thereIsATransaction(){

        if(AppdynamicsAgent.getTransaction()!=null){
            if("".equals(AppdynamicsAgent.getTransaction().getUniqueIdentifier())){
                return false;
            }
        }else{
            return false;
        }
        return true;
    }

    @Override
    public Object onMethodBegin(Object invokedObject, String s, String s1, Object[] paramValues) {

        if(thereIsATransaction()){
            return null;
        }


        KafkaDataHolder kafkaDataHolder = null;

        try {
            String correlationId = getCorrelationId(invokedObject, paramValues);
            Transaction transaction = AppdynamicsAgent.startTransaction("Kafka", correlationId, false);

            kafkaDataHolder = new KafkaDataHolder();
            kafkaDataHolder.setCorrelationId(correlationId);
            kafkaDataHolder.setTransaction(transaction);
        } catch(Exception e){

        }

        return kafkaDataHolder;
    }

    @Override
    public void onMethodEnd(Object o, Object o1, String s, String s1, Object[] objects, Throwable throwable, Object o2) {
        if(o!=null) {
            KafkaDataHolder kafkaDataHolder = (KafkaDataHolder) o;
            Set<DataScope> dataScopeSet = new HashSet(Arrays.asList(DataScope.ANALYTICS, DataScope.SNAPSHOTS));
            Matcher m = r.matcher(kafkaDataHolder.getCorrelationId());

            if (m.find()) {
                long btStartTime = Long.parseLong(m.group(1));
                String btId = m.group(2);
                String exitGuid = m.group(3);

                long resultTime = System.currentTimeMillis() - btStartTime;
                AppdynamicsAgent.getMetricPublisher().reportObservedMetric(btId + " " + exitGuid, resultTime);
                AppdynamicsAgent.getTransaction().collectData(btId + " " + exitGuid, "" + resultTime, dataScopeSet);
            }


            kafkaDataHolder.getTransaction().end();
        }

    }

    private void getProcessContext(Object invokedObject){
        if(key==null){
            Field[] fields = invokedObject.getClass().getDeclaredFields();
            for(Field f : fields){
                Class t = f.getType();
                if(t.getSimpleName().contains("ProcessorContext")){
                    key = getNewReflectionBuilder()
                            .accessFieldValue(f.getName(), true)
                            .build();
                }
            }
        }
    }

    private String getCorrelationId(Object invokedObject, Object[] paramValues) {
        String correlationId = "";

        try {
                Object pc = null;
                getProcessContext(invokedObject);

                try {
                    pc = this.key.execute(invokedObject.getClass().getClassLoader(), invokedObject);
                }catch(Exception e1){
                    key = null;
                    getProcessContext(invokedObject);
                    pc = this.key.execute(invokedObject.getClass().getClassLoader(), invokedObject);
                }


                if (pc != null) {
                    try {

                        Object in = this.getHeaders.execute(pc.getClass().getClassLoader(), pc);
                        Object lastHeader = this.lastHeader.execute(in.getClass().getClassLoader(), in, new Object[]{ITransactionDemarcator.APPDYNAMICS_TRANSACTION_CORRELATION_HEADER});
                        byte[] body = this.value.execute(in.getClass().getClassLoader(), lastHeader);
                        correlationId = new String(body);

                    } catch (Exception e) {
                    }
                }

        } catch (Exception f) {
        }

        return correlationId;
    }

}