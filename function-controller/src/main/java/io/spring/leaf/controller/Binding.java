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

package io.spring.leaf.controller;

/**
 * @author Mark Fisher
 */
public class Binding {

	private final String name;

	private final String function;

	private final String runner;

	private String input;

	private String output;

	public Binding(String name, String function, String runner) {
		this.name = name;
		this.function = function;
		this.runner = runner;
	}

	public String getName() {
		return name;
	}

	public String getFunction() {
		return function;
	}

	public String getRunner() {
		return runner;
	}

	public String getInput() {
		return input;
	}

	public void setInput(String input) {
		this.input = input;
	}

	public String getOutput() {
		return output;
	}

	public void setOutput(String output) {
		this.output = output;
	}

	@Override
	public String toString() {
		return "Binding [name=" + name + ", function=" + function + ", runner=" + runner + ", input=" + input
				+ ", output=" + output + "]";
	}
}
