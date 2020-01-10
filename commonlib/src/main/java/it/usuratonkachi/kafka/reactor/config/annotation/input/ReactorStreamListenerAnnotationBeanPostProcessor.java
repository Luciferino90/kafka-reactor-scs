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
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.function.Function;

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
		for (int i = 0; i < annotations.length; i++){
			Annotation[] anns = annotations[i];
			for (Annotation ann : anns) {
				if (Payload.class.equals(ann.annotationType()))
					payloadPosition = i;
				else if (Headers.class.equals(ann.annotationType()))
					headerPosition = i;
			}
		}
		return new ParamsPosition(payloadPosition, headerPosition);
	}

	private Object[] composeVarArgsParams(Method method, Message<?> message){
		ParamsPosition parameterPosition = getParameterPosition(method.getParameterAnnotations());
		Object[] params = new Object[2];
		params[parameterPosition.getPayloadPosition()] = message.getPayload();
		params[parameterPosition.getHeaderPosition()] = message.getHeaders();
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

}
