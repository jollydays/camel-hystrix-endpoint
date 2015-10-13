# camel-hystrix-endpoint

[![Build Status](https://travis-ci.org/jollydays/camel-hystrix-endpoint.svg?branch=master)](https://travis-ci.org/jollydays/camel-hystrix-endpoint)

Camel endpoint which wraps child endpoints into a synchronous or asynchronous hystrix circuit breaker.

This project allows easy integration of hystrix components into camel routes. A very simple example might look like this:

```java
	private class TestRoute extends RouteBuilder {

		@Override
		public void configure() throws Exception {
			onException(HystrixRuntimeException.class).handled(true).setBody().constant("error");
			
			from("direct:start").to("hystrix:mock:protectedRoute?hystrixGroup=test?hystrixCommand=slowCommand");
		}
	}
```
In this example the mock endpoint is protected by a hystrix endpoint by prefixing the mock endpoint with __hystrix__.
The _hystrixGroup_ parameter can be used to identify and group hystrix commands for monitoring in scaled environments.
The _hystrixCommand_ parameter can be used to identify hystrix commands for distributed configuration of components.
The _hystrixCommandTimeout_ parameter can be used to set the timeout for the hystrix command

When wrapping __direct__ endpoints with the hystrix component, whole sections of a route can be protected, including processors and endpoints:

```java
	private class TestRoute extends RouteBuilder {

		@Override
		public void configure() throws Exception {
			onException(HystrixRuntimeException.class).handled(true).setBody().constant("error");
			
			from("direct:start").to("hystrix:direct:protectedRoute?hystrixGroup=test&hystrixCommand=slowCommand");
			from("direct:protectedRoute").marsha().json().to("mock:result");
		}
	}
```

Binaries

Jars for Maven, Ivy, Gradle and others can be found at [http://search.maven.org](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.jollydays.camel%22%20AND%20a%3A%22camel-hystrix-endpoint%22).

Example for Maven:

```xml
<dependency>
  <groupId>com.jollydays.camel</groupId>
  <artifactId>camel-hystrix-endpoint</artifactId>
  <version>x.y.z</version>
</dependency>
```
