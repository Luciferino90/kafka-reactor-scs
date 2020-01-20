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

package it.usuratonkachi.kafka.reactor.config.annotation.input;

import it.usuratonkachi.kafka.reactor.config.ReactorStreamDispatcher;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.ClassUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static it.usuratonkachi.kafka.reactor.config.ReactorStreamDispatcher.mapToSingleElement;

/**
 * {@link BeanPostProcessor} that handles {@link ReactorStreamListener} annotations found on bean
 * methods.
 *
 * @see org.springframework.cloud.stream.binding.StreamListenerAnnotationBeanPostProcessor
 *
 */
@Component
@RequiredArgsConstructor
public class ReactorStreamListenerAnnotationBeanPostProcessor implements BeanPostProcessor, ApplicationContextAware {

	@AllArgsConstructor
	private static class ParamsPosition {
		@Getter
		private int payloadPosition;
		@Getter
		private int headerPosition;
		@Getter
		private Map<Integer, Header> headerMap;

		@AllArgsConstructor
		static class Header {
			@Getter
			private String value;
			@Getter
			private boolean required;
		}

	}

	private ConfigurableApplicationContext applicationContext;

	@Override
	public final void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
	}

	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public Object postProcessAfterInitialization(Object bean, String beanName) {
		Class<?> targetClass = AopUtils.isAopProxy(bean) ? AopUtils.getTargetClass(bean) : bean.getClass();

		if (Arrays.stream(targetClass.getMethods()).anyMatch(method -> method.isAnnotationPresent(ReactorStreamListener.class))) {
			Arrays.stream(targetClass.getMethods()).filter(method -> method.isAnnotationPresent(ReactorStreamListener.class))
					.forEach(method -> {
						String dispatcherName = method.getAnnotation(ReactorStreamListener.class).value();
						((ReactorStreamDispatcher) this.applicationContext.getBean(dispatcherName, MessageChannel.class)).listen(getFunctionListener(method, bean));
					});
		}
		return bean;
	}

	private ParamsPosition getParameterPosition(Annotation[][] annotations){
		int payloadPosition = -1;
		int headerPosition = -1;
		Map<Integer, ParamsPosition.Header> headerMap = new HashMap<>();
		for (int i = 0; i < annotations.length; i++){
			Annotation[] anns = annotations[i];
			for (Annotation ann : anns) {
				if (Payload.class.equals(ann.annotationType()))
					payloadPosition = i;
				else if (Headers.class.equals(ann.annotationType()))
					headerPosition = i;
				else if (Header.class.equals(ann.annotationType())) {
					headerMap.put(i, new ParamsPosition.Header(((Header)ann).value(), ((Header)ann).required()));
				}
			}
		}
		return new ParamsPosition(payloadPosition, headerPosition, headerMap);
	}

	private Object[] composeVarArgsParams(Method method, Message<?> message){
		ParamsPosition parameterPosition = getParameterPosition(method.getParameterAnnotations());
		Object[] params = new Object[2 + parameterPosition.getHeaderMap().size()];
		params[parameterPosition.getPayloadPosition()] = message.getPayload();
		List<String> extractedHeaders = new ArrayList<>();
		parameterPosition.getHeaderMap().forEach((key, value) -> {
			if (!message.getHeaders().containsKey(value.getValue()) && value.isRequired()) {
				throw new RuntimeException(String.format("Missing header %s from message", value));
			} else if (message.getHeaders().containsKey(value.getValue())) {
				params[key] = castHeader(method.getAnnotatedParameterTypes()[key].getType().getTypeName(), (byte[]) message.getHeaders().get(value.getValue()));
			} else {
				params[key] = defaultNullValue(method.getAnnotatedParameterTypes()[key].getType().getTypeName());
			}
			extractedHeaders.add(value.getValue());
		});
		params[parameterPosition.getHeaderPosition()] = message.getHeaders().entrySet()
				.stream()
				.filter(entry -> !extractedHeaders.contains(entry.getKey()))
				.collect(Collectors.groupingBy(Map.Entry::getKey, Collectors.mapping(Map.Entry::getValue, mapToSingleElement())));

		return params;
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private Function<Message<?>, Mono<Void>> getFunctionListener(Method method, Object targetBean){
		return message -> {
			try {
				return (Mono) method.invoke(targetBean, composeVarArgsParams(method, message));
			} catch (IllegalAccessException | InvocationTargetException ex) {
				throw new RuntimeException(ex);
			}
		};
	}

	private Object castHeader(String className, byte[] data){
		String swap = new String(data);
		if (swap.startsWith("\"") && swap.endsWith("\""))
			swap = swap.replace("\"", "");
		try {
			Class<?> clazz = ClassUtils.getClass(className);
			clazz = ClassUtils.isPrimitiveOrWrapper(ClassUtils.getClass(className)) ? ClassUtils.primitiveToWrapper(clazz) : clazz;
			if (clazz.equals(String.class)){
				return swap;
			} else if (clazz.equals(Long.class)) {
				return Long.valueOf(swap);
			} else if (clazz.equals(Integer.class)) {
				return Integer.valueOf(swap);
			} else {
				try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(data))) {
					return Class.forName(className).cast(in.readObject());
				}
			}
		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException(String.format("Header conversion for type %s not yet supported", className), e);
		}
	}

	private Object defaultNullValue(String className){
		try {
			Class<?> clazz = ClassUtils.getClass(className);
			if (clazz.isPrimitive()){
				clazz = ClassUtils.primitiveToWrapper(clazz);
				if (clazz.equals(Long.class)) {
					return 0L;
				} else if (clazz.equals(Integer.class)) {
					return 0;
				} else throw new ClassNotFoundException("No primitive found for class " + className);
			} else {
				return null;
			}
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(String.format("Header conversion for type %s not yet supported", className), e);
		}
	}

}
