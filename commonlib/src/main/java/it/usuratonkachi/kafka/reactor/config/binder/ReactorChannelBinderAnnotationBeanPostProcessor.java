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

package it.usuratonkachi.kafka.reactor.config.binder;

import it.usuratonkachi.kafka.reactor.config.ReactorKafkaProperties;
import it.usuratonkachi.kafka.reactor.config.ReactorStreamDispatcher;
import it.usuratonkachi.kafka.reactor.config.annotation.input.ReactorStreamListener;
import lombok.RequiredArgsConstructor;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.Ordered;
import org.springframework.core.PriorityOrdered;
import org.springframework.messaging.MessageChannel;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * {@link BeanPostProcessor} that handles {@link ReactorStreamListener} annotations found on bean
 * methods.
 *
 * @see org.springframework.cloud.stream.binding.StreamListenerAnnotationBeanPostProcessor
 *
 */
@Component
@RequiredArgsConstructor
public class ReactorChannelBinderAnnotationBeanPostProcessor implements BeanPostProcessor, ApplicationContextAware,
		PriorityOrdered {

	private ReactorKafkaProperties reactorKafkaProperties;

	private ConfigurableApplicationContext applicationContext;

	@Override
	public final void setApplicationContext(ApplicationContext applicationContext) {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
	}

	@Override
	public final Object postProcessAfterInitialization(Object bean, final String beanName) {
		Class<?> targetClass = AopUtils.isAopProxy(bean) ? AopUtils.getTargetClass(bean) : bean.getClass();
		if (targetClass.isAnnotationPresent(ReactorChannelBinder.class)){
			reactorKafkaProperties = this.applicationContext.getBean(ReactorKafkaProperties.class);
			Map<Class<?>, List<Field>> channels = Arrays.stream(targetClass.getDeclaredFields())
					.filter(field -> Arrays.stream(field.getDeclaredAnnotations()).anyMatch(annotation -> ReactorChannel.class.equals(annotation.annotationType())))
					.collect(Collectors.groupingBy(
							field -> Arrays.stream(field.getDeclaredAnnotations())
									.filter(annotation -> ReactorChannel.class.equals(annotation.annotationType()))
									.findFirst()
									.orElseThrow(() -> new IllegalStateException("Couldn't retrieve ReactorChannel annotation from " + beanName)).annotationType()
					));

			Optional.ofNullable(channels.get(ReactorChannel.class))
					.ifPresent(outputChannels ->
							outputChannels.forEach(outputChannel -> {
								String dispatcherName = outputChannel.getAnnotation(ReactorChannel.class).value();
								Class<?> dispatcherClass = outputChannel.getAnnotation(ReactorChannel.class).messageType();
								MessageChannel r = new ReactorStreamDispatcher<>(dispatcherClass, reactorKafkaProperties, dispatcherName);
								this.applicationContext.getBeanFactory().registerSingleton(dispatcherName, r);
							}));
		}
		return bean;
	}

	@Override
	public int getOrder() {
		return Ordered.LOWEST_PRECEDENCE - 11;
	}
}
