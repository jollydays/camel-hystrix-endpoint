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
 * HystrixProducer.java
 *
 * Authors:
 *     Roman Mohr (r.mohr@jollydays.com)
**/

package com.jollydays.camel;

import org.apache.camel.AsyncCallback;
import org.apache.camel.AsyncProcessor;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultAsyncProducer;
import org.apache.camel.util.ServiceHelper;

import rx.Observable;
import rx.Observer;
import rx.Subscriber;

import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.HystrixCommandProperties.Setter;
import com.netflix.hystrix.HystrixObservableCommand;

public class HystrixProducer extends DefaultAsyncProducer {

    private final Producer child;
    private final HystrixCommand.Setter commandSetter;
    private final HystrixObservableCommand.Setter observableCommandSetter;

    @Override
    public Exchange createExchange() {
        return child.createExchange();
    }

    @Override
    public Exchange createExchange(final ExchangePattern exchangePattern) {
        return child.createExchange(exchangePattern);
    }

    @Override
    @Deprecated
    public Exchange createExchange(final Exchange exchange) {
        return child.createExchange(exchange);
    }

    @Override
    public Endpoint getEndpoint() {
        return child.getEndpoint();
    }

    @Override
    public boolean isSingleton() {
        return false;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void process(final Exchange exchange) throws Exception {
        final HystrixCommand command = new HystrixCommand(commandSetter) {
            @Override
            protected Object run() throws Exception {
                child.process(exchange);
                return null;
            }
        };
        command.execute();
    }
    
    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public boolean process(final Exchange exchange, final AsyncCallback callback) {
        if (!(child instanceof AsyncProcessor)) {
            try {
                process(exchange);
            } catch (Exception e) {
                exchange.setException(e);
            }
            callback.done(true);
            return true;
        }
        
        final AsyncProcessor asyncChild = (AsyncProcessor) child;
        
        final HystrixObservableCommand command = new HystrixObservableCommand(observableCommandSetter) {

            @Override
            protected Observable construct() {
                return Observable.create(new Observable.OnSubscribe<Object>() {
                    @Override
                    public void call(final Subscriber<? super Object> observer) {
                        try {
                            asyncChild.process(exchange, new AsyncCallback() {
                                
                                @Override
                                public void done(boolean doneSync) {
                                    observer.onCompleted();
                                }
                            });
                        } catch (Exception e) {
                            observer.onError(e);
                        }
                    }
                 });
            }
        };
        
        command.observe().subscribe(new Observer() {

            @Override
            public void onCompleted() {
                callback.done(false);
            }

            @Override
            public void onError(Throwable e) {
                exchange.setException(e);
                callback.done(false);
            }

            @Override
            public void onNext(Object t) {
            }
        });
        return false;
    }
    

    public HystrixProducer(final Endpoint endpoint, final Producer child, final String group, final String command, final Integer timeout) {
        super(endpoint);
        this.child = child;
        HystrixCommandGroupKey groupKey = HystrixCommandGroupKey.Factory.asKey(group);
        commandSetter = HystrixCommand.Setter.withGroupKey(groupKey);
        observableCommandSetter = HystrixObservableCommand.Setter.withGroupKey(groupKey);
        if (command != null) {
            HystrixCommandKey commandKey = HystrixCommandKey.Factory.asKey(command);
            commandSetter.andCommandKey(commandKey);
            observableCommandSetter.andCommandKey(commandKey);
        }
        if(timeout != null) {
            Setter commandProperties = HystrixCommandProperties.Setter().withExecutionTimeoutInMilliseconds(timeout);
            commandSetter.andCommandPropertiesDefaults(commandProperties);
            observableCommandSetter.andCommandPropertiesDefaults(commandProperties);
        }
    }

    @Override
    protected void doStart() throws Exception {
        ServiceHelper.startService(child);
        super.doStart();
    }

    @Override
    protected void doStop() throws Exception {
        ServiceHelper.stopService(child);
        super.doStop();
    }

    @Override
    protected void doSuspend() throws Exception {
        ServiceHelper.suspendService(child);
        super.doSuspend();
    }

    @Override
    protected void doResume() throws Exception {
        ServiceHelper.resumeService(child);
        super.doResume();
    }

    @Override
    protected void doShutdown() throws Exception {
        ServiceHelper.stopAndShutdownService(child);
        super.doShutdown();
    }

}
