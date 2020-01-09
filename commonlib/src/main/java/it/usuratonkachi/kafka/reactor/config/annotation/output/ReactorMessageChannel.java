/*
 * Copyright 2016-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package it.usuratonkachi.kafka.reactor.config.annotation.output;

import org.springframework.core.annotation.AliasFor;
import org.springframework.messaging.handler.annotation.MessageMapping;

import java.lang.annotation.*;

/**
 *
 * @see org.springframework.cloud.stream.annotation.StreamListener
 *
*/
@Target({ ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
@MessageMapping
@Documented
public @interface ReactorMessageChannel {

	/**
	 * The name of the binding target (e.g. channel) that the method subscribes to.
	 * @return the name of the binding target.
	 */
	@AliasFor("target") String value() default "";

	/**
	 * The name of the binding target (e.g. channel) that the method subscribes to.
	 * @return the name of the binding target.
	 */
	@AliasFor("value") String target() default "";

	/**
	 * A condition that must be met by all items that are dispatched to this method.
	 * @return a SpEL expression that must evaluate to a {@code boolean} value.
	 */
	String condition() default "";

	/**
	 * When "true" (default), and a {@code @SendTo} annotation is present, copy the
	 * inbound headers to the outbound message (if the header is absent on the outbound
	 * message). Can be an expression ({@code #{...}}) or property placeholder. Must
	 * resolve to a boolean or a string that is parsed by {@code Boolean.parseBoolean()}.
	 * An expression that resolves to {@code null} is interpreted to mean {@code false}.
	 *
	 * The expression is evaluated during application initialization, and not for each
	 * individual message.
	 *
	 * Prior to version 1.3.0, the default value used to be "false" and headers were not
	 * propagated by default.
	 *
	 * Starting with version 1.3.0, the default value is "true".
	 *
	 * @since 1.2.3
	 * @return {@link Boolean} in a String format
	 */
	String copyHeaders() default "true";

}
