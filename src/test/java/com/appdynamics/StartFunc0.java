package com.appdynamics;

import com.appdynamics.agent.api.AppdynamicsAgent;
import com.appdynamics.instrumentation.sdk.Rule;
import com.appdynamics.instrumentation.sdk.SDKClassMatchType;
import com.appdynamics.instrumentation.sdk.SDKStringMatchType;
import com.appdynamics.instrumentation.sdk.template.AGenericInterceptor;
import com.appdynamics.instrumentation.sdk.toolbox.reflection.IReflector;

import java.util.ArrayList;
import java.util.List;

/**
 * created by haojun.li on 7/10/18
 */
public class StartFunc0 extends AGenericInterceptor {
    IReflector id = null;

    private static final String CLASS_TO_INSTRUMENT = "rx.functions.Func0";
    private static final String METHOD_TO_INSTRUMENT = "<init>";

    public StartFunc0(){
        super();
        id = getNewReflectionBuilder()
                .invokeInstanceMethod("id", true).build();
    }

    @Override
    public Object onMethodBegin(Object o, String s, String s1, Object[] objects) {
        AppdynamicsAgent.getTransaction().markHandoff(o);
        getLogger().info(""+System.identityHashCode(o));
        return null;
    }

    @Override
    public void onMethodEnd(Object o, Object o1, String s, String s1, Object[] objects, Throwable throwable, Object o2) {

    }

    @Override
    public List<Rule> initializeRules() {
        List<Rule> result = new ArrayList<>();

        Rule.Builder bldr = new Rule.Builder(CLASS_TO_INSTRUMENT);
        bldr = bldr.classMatchType(SDKClassMatchType.IMPLEMENTS_INTERFACE).classStringMatchType(SDKStringMatchType.EQUALS);
        bldr = bldr.methodMatchString(METHOD_TO_INSTRUMENT).methodStringMatchType(SDKStringMatchType.EQUALS);
        result.add(bldr.build());
        return result;
    }


}