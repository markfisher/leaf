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

package io.spring.leaf.invoker;

import java.util.function.Function;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.messaging.Message;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

import reactor.core.publisher.Flux;

/**
 * @author Mark Fisher
 */
@EnableBinding(Sink.class)
@ConditionalOnProperty(value = "spring.cloud.stream.bindings.output.destination", matchIfMissing = true)
public class FunctionInvokingSink {

	@Autowired
	private Function<Flux<String>, Flux<String>> targetFunction;

	private final RestTemplate restTemplate = new RestTemplate();

	@StreamListener(Sink.INPUT)
	public void handle(Message<String> message) {
		String output = targetFunction.apply(Flux.just(message.getPayload())).blockFirst();
		String replyTo = message.getHeaders().get("gatewayReplyTo", String.class);
		if (!StringUtils.isEmpty(replyTo)) {
			this.restTemplate.postForObject(replyTo, output, String.class);			
		}
		else {
			System.out.println("no replyTo URL available for output: " + output);
		}
	}
}
