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
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;

import reactor.core.publisher.Flux;

/**
 * @author Mark Fisher
 */
@EnableBinding(Processor.class)
@ConditionalOnProperty(value = "spring.cloud.stream.bindings.output.destination")
public class FunctionInvokingProcessor {

	@Autowired
	private Function<Flux<String>, Flux<String>> targetFunction;

	@StreamListener(Processor.INPUT)
	@SendTo(Processor.OUTPUT)
	public Message<String> handle(Message<String> message) {
		String output = targetFunction.apply(Flux.just(message.getPayload())).blockFirst();
		return MessageBuilder.withPayload(output).copyHeadersIfAbsent(message.getHeaders()).build();
	}
}
