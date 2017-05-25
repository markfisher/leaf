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

package io.spring.leaf.bootstrap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.cloud.stream.messaging.Sink;

import io.spring.leaf.invoker.FunctionConfiguration;
import io.spring.leaf.invoker.FunctionInvokingProcessor;
import io.spring.leaf.invoker.FunctionInvokingSink;

/**
 * @author Mark Fisher
 */
@EnableBinding(Sink.class)
public class FunctionBootstrappingListener {

	@Autowired
	private BindingService bindingService;

	@StreamListener(Sink.INPUT)
	public void handle(Map<String, String> deploymentRequest) throws IOException {
		this.bindingService.unbindConsumers(Sink.INPUT);
		List<String> args = new ArrayList<>();
		args.add("--spring.cloud.faas.function.resource=" + deploymentRequest.get("function"));
		args.add("--spring.cloud.stream.bindings.input.destination=" + deploymentRequest.get("input"));
		args.add("--spring.cloud.stream.bindings.input.group=default");
		Class<?> functionInvokerClass = FunctionInvokingSink.class;
		if (deploymentRequest.get("output") != null) {
			args.add("--spring.cloud.stream.bindings.output.destination=" + deploymentRequest.get("output"));
			functionInvokerClass = FunctionInvokingProcessor.class;
		}
		new SpringApplicationBuilder(functionInvokerClass, FunctionConfiguration.class)
				.web(false) // parent?
				.run(args.toArray(new String[args.size()]));
	}
}
