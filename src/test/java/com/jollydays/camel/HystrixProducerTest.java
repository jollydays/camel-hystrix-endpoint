/*
 * Copyright 2015, Jollydays GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * HystrixProducerTest.java
 *
 * Authors:
 *     Roman Mohr (r.mohr@jollydays.com)
**/

package com.jollydays.camel;

import com.netflix.config.ConfigurationManager;
import com.netflix.hystrix.exception.HystrixRuntimeException;
import org.apache.camel.CamelContext;
import org.apache.camel.EndpointInject;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.Before;
import org.junit.Test;

public class HystrixProducerTest extends CamelTestSupport {

    public static final String DEFAULT_CIRCUIT_BREAKER_FORCE_OPEN = "hystrix.command.default.circuitBreaker.forceOpen";
    public static final String TEST_CIRCUIT_BREAKER_FORCE_OPEN = "hystrix.command.test.circuitBreaker.forceOpen";
    public static final String SLOW_COMMAND_TIMEOUT = "hystrix.command.slowCommand.execution.isolation.thread.timeoutInMilliseconds";

    @Produce(uri = "direct:start")
    protected ProducerTemplate template;

    @Produce(uri = "direct:protectedSlowRoute")
    protected ProducerTemplate slowRouteTemplate;

    @EndpointInject(uri = "mock:result")
    protected MockEndpoint resultEndpoint;

    @EndpointInject(uri = "mock:slowResult")
    protected MockEndpoint slowResultEndpoint;

    @Produce(uri = "direct:slowRouteWithSmallTimeout")
    protected ProducerTemplate slowRouteWithSmallTimeoutTemplate;

    @Produce(uri = "direct:slowRouteWithBigTimeout")
    protected ProducerTemplate slowRouteWithBigTimeoutTemplate;

    private class TestRoute extends RouteBuilder {

        @Override
        public void configure() throws Exception {
            onException(HystrixRuntimeException.class).handled(true).setBody().constant("error");
            from("direct:start").to("hystrix:direct:mitm?hystrixGroup=testGroup");
            from("direct:mitm").to("hystrix:direct:final?hystrixGroup=testGroup&hystrixCommand=test");
            from("direct:final").to("mock:result");
            from("direct:protectedSlowRoute").to("hystrix:direct:slowRoute?hystrixGroup=testGroup&hystrixCommand=slowCommand");
            from("direct:slowRoute").delayer(1000).to("mock:slowResult");

            // slow create a slow route that has a lower timeout than the default so we can be sure that the
            // hystrixCommandTimeout parameter overrides the default set in @Before
            from("direct:slowRouteWithSmallTimeout").to("hystrix:direct:slowRouteForParameterTimeout?hystrixGroup=testGroup&hystrixCommand=slowCommandViaParamST&hystrixCommandTimeout=100");
            from("direct:slowRouteWithBigTimeout").to("hystrix:direct:slowRouteForParameterTimeout?hystrixGroup=testGroup&hystrixCommand=slowCommandViaParamBT&hystrixCommandTimeout=300");
            from("direct:slowRouteForParameterTimeout").delayer(200).to("mock:slowResult");
        }
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        ConfigurationManager.getConfigInstance().setProperty(DEFAULT_CIRCUIT_BREAKER_FORCE_OPEN, "false");
        ConfigurationManager.getConfigInstance().setProperty(TEST_CIRCUIT_BREAKER_FORCE_OPEN, "false");
        ConfigurationManager.getConfigInstance().setProperty(SLOW_COMMAND_TIMEOUT, 500);
    }

    @Override
    protected CamelContext createCamelContext() throws Exception {
        CamelContext context = super.createCamelContext();
        return context;
    }

    @Override
    protected RouteBuilder createRouteBuilder() throws Exception {
        return new TestRoute();
    }

    @Test
    public void shouldUseHystrixProducer() throws InterruptedException {
        resultEndpoint.expectedBodiesReceived("test");
        template.sendBody("test");
        assertMockEndpointsSatisfied();
    }

    @Test
    public void shouldReactOnOpenCircuit() throws InterruptedException {
        ConfigurationManager.getConfigInstance().setProperty(TEST_CIRCUIT_BREAKER_FORCE_OPEN, "true");
        final Object response = template.requestBody("test");
        assertEquals("error", response);
    }

    @Test
    public void shouldRespectMaxExecutionTime() throws InterruptedException {
        slowResultEndpoint.expectedBodiesReceived("test", "test");
        final Object tooSlow = slowRouteTemplate.requestBody("test");
        assertEquals("error", tooSlow);
        ConfigurationManager.getConfigInstance().setProperty(SLOW_COMMAND_TIMEOUT, 3000);
        final Object fastEnough = slowRouteTemplate.requestBody("test");
        assertEquals("test", fastEnough);
        slowResultEndpoint.assertIsSatisfied();
    }

    @Test
    public void shouldRespectMaxExecutionTimeWithParamter() throws InterruptedException {
        final Object tooSlowWithProperty = slowRouteWithSmallTimeoutTemplate.requestBody("test");
        assertEquals("error", tooSlowWithProperty);
        final Object fastEnough = slowRouteWithBigTimeoutTemplate.requestBody("test");
        assertEquals("test", fastEnough);
    }
}
