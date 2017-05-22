/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.spring.leaf.runner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.function.compiler.proxy.ByteCodeLoadingFunction;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import io.spring.leaf.invoker.FunctionInvokingProcessor;
import io.spring.leaf.invoker.FunctionInvokingSink;
import reactor.core.publisher.Flux;

/**
 * @author Mark Fisher
 */
@EnableBinding(Sink.class)
public class FunctionBootstrappingListener {

	@Autowired
	private BindingService bindingService;

	@StreamListener(Sink.INPUT)
	public void handle(Map<String, String> deploymentRequest) throws IOException {
		this.bindingService.unbindConsumers("input");
		List<String> args = new ArrayList<>();
		args.add("--spring.cloud.faas.function.resource=" + deploymentRequest.get("function"));
		args.add("--spring.cloud.stream.bindings.input.destination=function-" + deploymentRequest.get("input"));
		args.add("--spring.cloud.stream.bindings.input.group=default");
		Class<?> functionInvokerClass = FunctionInvokingSink.class;
		if (deploymentRequest.get("output") != null) {
			args.add("--spring.cloud.stream.bindings.output.destination=function-" + deploymentRequest.get("output"));
			functionInvokerClass = FunctionInvokingProcessor.class;
		}
		new SpringApplicationBuilder(functionInvokerClass, FunctionConfiguration.class)
				.web(false) // parent?
				.run(args.toArray(new String[args.size()]));
	}

	@Configuration
	@ConditionalOnProperty("spring.cloud.faas.function.resource")
	public static class FunctionConfiguration {

		@Value("${spring.cloud.faas.function.resource}")
		public Resource resource;

		@Bean
		public ByteCodeLoadingFunction<Flux<String>, Flux<String>> targetFunction() {
			return new ByteCodeLoadingFunction<>(this.resource);
		}
	}
}
