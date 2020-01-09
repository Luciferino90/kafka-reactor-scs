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

import it.usuratonkachi.kafka.reactor.config.ReactorKafkaProperties;
import it.usuratonkachi.kafka.reactor.config.ReactorStreamDispatcher;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.binding.StreamListenerErrorMessages;
import org.springframework.cloud.stream.binding.StreamListenerParameterAdapter;
import org.springframework.cloud.stream.binding.StreamListenerResultAdapter;
import org.springframework.cloud.stream.config.SpringIntegrationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.MethodParameter;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.handler.AbstractReplyProducingMessageHandler;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.core.DestinationResolver;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.stereotype.Component;
import org.springframework.util.*;
import reactor.core.publisher.Mono;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Function;
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
public class ReactorStreamListenerAnnotationBeanPostProcessor implements BeanPostProcessor, ApplicationContextAware,
		SmartInitializingSingleton {

	@AllArgsConstructor
	private static class ParamsPosition {
		@Getter
		private int payloadPosition;
		@Getter
		private int headerPosition;
	}

	private final ReactorKafkaProperties reactorKafkaProperties;

	private static final SpelExpressionParser SPEL_EXPRESSION_PARSER = new SpelExpressionParser();

	// @checkstyle:off
	private final MultiValueMap<String, ReactorStreamListenerHandlerMethodMapping> mappedListenerMethods = new LinkedMultiValueMap<>();

	// @checkstyle:on

	private final Set<Runnable> streamListenerCallbacks = new HashSet<>();

	// == dependencies that are injected in 'afterSingletonsInstantiated' to avoid early
	// initialization
	private DestinationResolver<MessageChannel> binderAwareChannelResolver;

	private MessageHandlerMethodFactory messageHandlerMethodFactory;

	// == end dependencies
	private SpringIntegrationProperties springIntegrationProperties;

	private ConfigurableApplicationContext applicationContext;

	private BeanExpressionResolver resolver;

	private BeanExpressionContext expressionContext;

	private Set<ReactorStreamListenerSetupMethodOrchestrator> streamListenerSetupMethodOrchestrators = new LinkedHashSet<>();

	private boolean streamListenerPresent;

	@Override
	public final void setApplicationContext(ApplicationContext applicationContext)
			throws BeansException {
		this.applicationContext = (ConfigurableApplicationContext) applicationContext;
		this.resolver = this.applicationContext.getBeanFactory()
				.getBeanExpressionResolver();
		this.expressionContext = new BeanExpressionContext(
				this.applicationContext.getBeanFactory(), null);
	}

	@Override
	@SuppressWarnings({"unchecked"})
	public final void afterSingletonsInstantiated() {
		if (!this.streamListenerPresent) {
			return;
		}
		this.injectAndPostProcessDependencies();
		EvaluationContext evaluationContext = IntegrationContextUtils
				.getEvaluationContext(this.applicationContext.getBeanFactory());
		for (Map.Entry<String, List<ReactorStreamListenerHandlerMethodMapping>> mappedBindingEntry : this.mappedListenerMethods
				.entrySet()) {
			ArrayList<DispatchingReactorStreamListenerMessageHandler.ConditionalStreamListenerMessageHandlerWrapper> handlers;
			handlers = new ArrayList<>();
			for (ReactorStreamListenerHandlerMethodMapping mapping : mappedBindingEntry
					.getValue()) {
				final InvocableHandlerMethod invocableHandlerMethod = this.messageHandlerMethodFactory
						.createInvocableHandlerMethod(mapping.getTargetBean(),
								checkProxy(mapping.getMethod(), mapping.getTargetBean()));
				ReactorStreamListenerMessageHandler streamListenerMessageHandler = new ReactorStreamListenerMessageHandler(
						invocableHandlerMethod,
						resolveExpressionAsBoolean(mapping.getCopyHeaders(),
								"copyHeaders"),
						this.springIntegrationProperties
								.getMessageHandlerNotPropagatedHeaders());
				streamListenerMessageHandler
						.setApplicationContext(this.applicationContext);
				streamListenerMessageHandler
						.setBeanFactory(this.applicationContext.getBeanFactory());
				if (StringUtils.hasText(mapping.getDefaultOutputChannel())) {
					streamListenerMessageHandler
							.setOutputChannelName(mapping.getDefaultOutputChannel());
				}
				streamListenerMessageHandler.afterPropertiesSet();
				if (StringUtils.hasText(mapping.getCondition())) {
					String conditionAsString = resolveExpressionAsString(
							mapping.getCondition(), "condition");
					Expression condition = SPEL_EXPRESSION_PARSER
							.parseExpression(conditionAsString);
					handlers.add(
							new DispatchingReactorStreamListenerMessageHandler.ConditionalStreamListenerMessageHandlerWrapper(
									condition, streamListenerMessageHandler));
				}
				else {
					handlers.add(
							new DispatchingReactorStreamListenerMessageHandler.ConditionalStreamListenerMessageHandlerWrapper(
									null, streamListenerMessageHandler));
				}
			}
			if (handlers.size() > 1) {
				for (DispatchingReactorStreamListenerMessageHandler.ConditionalStreamListenerMessageHandlerWrapper handler : handlers) {
					Assert.isTrue(handler.isVoid(),
							StreamListenerErrorMessages.MULTIPLE_VALUE_RETURNING_METHODS);
				}
			}
			AbstractReplyProducingMessageHandler handler;

			if (handlers.size() > 1 || handlers.get(0).getCondition() != null) {
				handler = new DispatchingReactorStreamListenerMessageHandler(handlers,
						evaluationContext);
			}
			else {
				handler = handlers.get(0).getStreamListenerMessageHandler();
			}
			handler.setApplicationContext(this.applicationContext);
			handler.setChannelResolver(this.binderAwareChannelResolver);
			handler.afterPropertiesSet();
			this.applicationContext.getBeanFactory().registerSingleton(
					handler.getClass().getSimpleName() + handler.hashCode(), handler);

			ReactorStreamListenerHandlerMethodMapping listener = this.mappedListenerMethods.getFirst(mappedBindingEntry.getKey());

			if (listener == null || listener.getMethod() == null) throw new RuntimeException("Cannot find method " + mappedBindingEntry.getKey());

			Class<?> clazz = getPayloadType(listener.getMethod());

			getReactorStreamDispatcher(clazz, mappedBindingEntry.getKey())
					.listen(getFunctionListener(listener));
		}
		this.mappedListenerMethods.clear();
	}

	private Class<?> getPayloadType(Method method){
		Integer pos = getPayloadPosition(method.getParameterAnnotations());
		return method.getParameterTypes()[pos];

	}

	private Integer getPayloadPosition(Annotation[][] annotations){
		for (int i = 0; i < annotations.length; i++){
			Annotation[] anns = annotations[i];
			for (Annotation ann : anns) {
				if (Payload.class.equals(ann.annotationType()))
					return i;
			}
		}
		throw new RuntimeException("No Payload annotation found!");
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
	private Function<Message<?>, Mono<Void>> getFunctionListener(ReactorStreamListenerHandlerMethodMapping listener){
		return message -> {
			try {
				return (Mono) listener.getMethod().invoke(listener.getTargetBean(), composeVarArgsParams(listener.getMethod(), message));
			} catch (IllegalAccessException | InvocationTargetException ex) {
				throw new RuntimeException(ex);
			}
		};
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	private ReactorStreamDispatcher getReactorStreamDispatcher(Class clazz, String channel){
		return new ReactorStreamDispatcher(clazz, reactorKafkaProperties, channel);
	}

	@Override
	public final Object postProcessAfterInitialization(Object bean, final String beanName)
			throws BeansException {
		if ("puppa".equalsIgnoreCase(beanName))
			System.out.println("Stop");
		System.out.println("FANCIU: " + beanName);
		Class<?> targetClass = AopUtils.isAopProxy(bean) ? AopUtils.getTargetClass(bean)
				: bean.getClass();
		Field[] fields = Arrays.stream(ReflectionUtils.getDeclaredFields(targetClass)).filter(field -> field.getAnnotatedType() != null).collect(;Collectors.toList())
		for (Method method : uniqueDeclaredMethods) {
			ReactorStreamListener streamListener = AnnotatedElementUtils
					.findMergedAnnotation(method, ReactorStreamListener.class);
			if (streamListener != null && !method.isBridge()) {
				this.streamListenerPresent = true;
				this.streamListenerCallbacks.add(() -> {
					Assert.isTrue(method.getAnnotation(Input.class) == null,
							StreamListenerErrorMessages.INPUT_AT_STREAM_LISTENER);
					this.doPostProcess(streamListener, method, bean);
				});
			}
		}
		return bean;
	}

	/**
	 * Extension point, allowing subclasses to customize the {@link ReactorStreamListener}
	 * annotation detected by the postprocessor.
	 * @param originalAnnotation the original annotation
	 * @param annotatedMethod the method on which the annotation has been found
	 * @return the postprocessed {@link ReactorStreamListener} annotation
	 */
	protected ReactorStreamListener postProcessAnnotation(ReactorStreamListener  originalAnnotation,
			Method annotatedMethod) {
		return originalAnnotation;
	}

	private void doPostProcess(ReactorStreamListener reactorStreamListener, Method method,
			Object bean) {
		reactorStreamListener = postProcessAnnotation(reactorStreamListener, method);
		Optional<ReactorStreamListenerSetupMethodOrchestrator> orchestratorOptional;
		orchestratorOptional = this.streamListenerSetupMethodOrchestrators.stream()
				.filter(t -> t.supports(method)).findFirst();
		Assert.isTrue(orchestratorOptional.isPresent(),
				"A matching StreamListenerSetupMethodOrchestrator must be present");
		ReactorStreamListenerSetupMethodOrchestrator streamListenerSetupMethodOrchestrator = orchestratorOptional
				.get();
		streamListenerSetupMethodOrchestrator
				.orchestrateStreamListenerSetupMethod(reactorStreamListener, method, bean);
	}

	private Method checkProxy(Method methodArg, Object bean) {
		Method method = methodArg;
		if (AopUtils.isJdkDynamicProxy(bean)) {
			try {
				// Found a @StreamListener method on the target class for this JDK proxy
				// ->
				// is it also present on the proxy itself?
				method = bean.getClass().getMethod(method.getName(),
						method.getParameterTypes());
				Class<?>[] proxiedInterfaces = ((Advised) bean).getProxiedInterfaces();
				for (Class<?> iface : proxiedInterfaces) {
					try {
						method = iface.getMethod(method.getName(),
								method.getParameterTypes());
						break;
					}
					catch (NoSuchMethodException noMethod) {
					}
				}
			}
			catch (SecurityException ex) {
				ReflectionUtils.handleReflectionException(ex);
			}
			catch (NoSuchMethodException ex) {
				throw new IllegalStateException(String.format(
						"@StreamListener method '%s' found on bean target class '%s', "
								+ "but not found in any interface(s) for bean JDK proxy. Either "
								+ "pull the method up to an interface or switch to subclass (CGLIB) "
								+ "proxies by setting proxy-target-class/proxyTargetClass attribute to 'true'",
						method.getName(), method.getDeclaringClass().getSimpleName()),
						ex);
			}
		}
		return method;
	}

	private String resolveExpressionAsString(String value, String property) {
		Object resolved = resolveExpression(value);
		if (resolved instanceof String) {
			return (String) resolved;
		}
		else {
			throw new IllegalStateException("Resolved " + property + " to ["
					+ resolved.getClass() + "] instead of String for [" + value + "]");
		}
	}

	private boolean resolveExpressionAsBoolean(String value, String property) {
		Object resolved = resolveExpression(value);
		if (resolved == null) {
			return false;
		}
		else if (resolved instanceof String) {
			return Boolean.parseBoolean((String) resolved);
		}
		else if (resolved instanceof Boolean) {
			return (Boolean) resolved;
		}
		else {
			throw new IllegalStateException(
					"Resolved " + property + " to [" + resolved.getClass()
							+ "] instead of String or Boolean for [" + value + "]");
		}
	}

	private String resolveExpression(String value) {
		String resolvedValue = this.applicationContext.getBeanFactory()
				.resolveEmbeddedValue(value);
		if (resolvedValue.startsWith("#{") && value.endsWith("}")) {
			resolvedValue = (String) this.resolver.evaluate(resolvedValue,
					this.expressionContext);
		}
		return resolvedValue;
	}

	/**
	 * This operations ensures that required dependencies are not accidentally injected
	 * early given that this bean is BPP.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void injectAndPostProcessDependencies() {
		Collection<StreamListenerParameterAdapter> streamListenerParameterAdapters = this.applicationContext
				.getBeansOfType(StreamListenerParameterAdapter.class).values();
		Collection<StreamListenerResultAdapter> streamListenerResultAdapters = this.applicationContext
				.getBeansOfType(StreamListenerResultAdapter.class).values();
		this.binderAwareChannelResolver = this.applicationContext
				.getBean("binderAwareChannelResolver", DestinationResolver.class);
		this.messageHandlerMethodFactory = this.applicationContext
				.getBean(MessageHandlerMethodFactory.class);
		this.springIntegrationProperties = this.applicationContext
				.getBean(SpringIntegrationProperties.class);

		this.streamListenerSetupMethodOrchestrators.addAll(this.applicationContext
				.getBeansOfType(ReactorStreamListenerSetupMethodOrchestrator.class).values());

		// Default orchestrator for StreamListener method invocation is added last into
		// the LinkedHashSet.
		this.streamListenerSetupMethodOrchestrators.add(
				new DefaultReactorStreamListenerSetupMethodOrchestrator(this.applicationContext,
						streamListenerParameterAdapters, streamListenerResultAdapters));

		this.streamListenerCallbacks.forEach(Runnable::run);
	}

	private class ReactorStreamListenerHandlerMethodMapping {

		private final Object targetBean;

		private final Method method;

		private final String condition;

		private final String defaultOutputChannel;

		private final String copyHeaders;

		ReactorStreamListenerHandlerMethodMapping(Object targetBean, Method method,
				String condition, String defaultOutputChannel, String copyHeaders) {
			this.targetBean = targetBean;
			this.method = method;
			this.condition = condition;
			this.defaultOutputChannel = defaultOutputChannel;
			this.copyHeaders = copyHeaders;
		}

		Object getTargetBean() {
			return this.targetBean;
		}

		Method getMethod() {
			return this.method;
		}

		String getCondition() {
			return this.condition;
		}

		String getDefaultOutputChannel() {
			return this.defaultOutputChannel;
		}

		public String getCopyHeaders() {
			return this.copyHeaders;
		}

	}

	@SuppressWarnings("rawtypes")
	private final class DefaultReactorStreamListenerSetupMethodOrchestrator
			implements ReactorStreamListenerSetupMethodOrchestrator {

		private final ConfigurableApplicationContext applicationContext;

		private final Collection<StreamListenerParameterAdapter> streamListenerParameterAdapters;

		private final Collection<StreamListenerResultAdapter> streamListenerResultAdapters;

		private DefaultReactorStreamListenerSetupMethodOrchestrator(
				ConfigurableApplicationContext applicationContext,
				Collection<StreamListenerParameterAdapter> streamListenerParameterAdapters,
				Collection<StreamListenerResultAdapter> streamListenerResultAdapters) {
			this.applicationContext = applicationContext;
			this.streamListenerParameterAdapters = streamListenerParameterAdapters;
			this.streamListenerResultAdapters = streamListenerResultAdapters;
		}

		@Override
		public void orchestrateStreamListenerSetupMethod(ReactorStreamListener streamListener,
				Method method, Object bean) {
			String methodAnnotatedInboundName = streamListener.value();

			String methodAnnotatedOutboundName = ReactorStreamListenerMethodUtils
					.getOutboundBindingTargetName(method);
			int inputAnnotationCount = ReactorStreamListenerMethodUtils
					.inputAnnotationCount(method);
			int outputAnnotationCount = ReactorStreamListenerMethodUtils
					.outputAnnotationCount(method);
			boolean isDeclarative = checkDeclarativeMethod(method,
					methodAnnotatedInboundName, methodAnnotatedOutboundName);
			ReactorStreamListenerMethodUtils.validateStreamListenerMethod(method,
					inputAnnotationCount, outputAnnotationCount,
					methodAnnotatedInboundName, methodAnnotatedOutboundName,
					isDeclarative, streamListener.condition());
			if (isDeclarative) {
				StreamListenerParameterAdapter[] toSlpaArray;
				toSlpaArray = new StreamListenerParameterAdapter[this.streamListenerParameterAdapters
						.size()];
				Object[] adaptedInboundArguments = adaptAndRetrieveInboundArguments(
						method, methodAnnotatedInboundName, this.applicationContext,
						this.streamListenerParameterAdapters.toArray(toSlpaArray));
				invokeReactorStreamListenerResultAdapter(method, bean,
						methodAnnotatedOutboundName, adaptedInboundArguments);
			}
			else {
				registerHandlerMethodOnListenedChannel(method, streamListener, bean);
			}
		}

		@Override
		public boolean supports(Method method) {
			// default catch all orchestrator
			return true;
		}

		@SuppressWarnings("unchecked")
		private void invokeReactorStreamListenerResultAdapter(Method method, Object bean,
				String outboundName, Object... arguments) {
			try {
				if (Void.TYPE.equals(method.getReturnType())) {
					method.invoke(bean, arguments);
				}
				else {
					Object result = method.invoke(bean, arguments);
					if (!StringUtils.hasText(outboundName)) {
						for (int parameterIndex = 0; parameterIndex < method
								.getParameterTypes().length; parameterIndex++) {
							MethodParameter methodParameter = MethodParameter
									.forExecutable(method, parameterIndex);
							if (methodParameter.hasParameterAnnotation(Output.class)) {
								outboundName = methodParameter
										.getParameterAnnotation(Output.class).value();
							}
						}
					}
					Object targetBean = this.applicationContext.getBean(outboundName);
					for (StreamListenerResultAdapter streamListenerResultAdapter : this.streamListenerResultAdapters) {
						if (streamListenerResultAdapter.supports(result.getClass(),
								targetBean.getClass())) {
							streamListenerResultAdapter.adapt(result, targetBean);
							break;
						}
					}
				}
			}
			catch (Exception e) {
				throw new BeanInitializationException(
						"Cannot setup StreamListener for " + method, e);
			}
		}

		private void registerHandlerMethodOnListenedChannel(
				Method method, ReactorStreamListener streamListener, Object bean) {
			Assert.hasText(streamListener.value(), "The binding name cannot be null");
			if (!StringUtils.hasText(streamListener.value())) {
				throw new BeanInitializationException(
						"A bound component name must be specified");
			}
			final String defaultOutputChannel = ReactorStreamListenerMethodUtils
					.getOutboundBindingTargetName(method);
			if (Mono.class.equals(method.getReturnType())) {
				Assert.isTrue(StringUtils.isEmpty(defaultOutputChannel),
						"An output channel cannot be specified for a method that does not return a value");
			}
			else {
				Assert.isTrue(!StringUtils.isEmpty(defaultOutputChannel),
						"An output channel must be specified for a method that can return a value");
			}
			ReactorStreamListenerMethodUtils.validateStreamListenerMessageHandler(method);
			ReactorStreamListenerAnnotationBeanPostProcessor.this.mappedListenerMethods.add(
					streamListener.value(),
					new ReactorStreamListenerHandlerMethodMapping(bean, method,
							streamListener.condition(), defaultOutputChannel,
							streamListener.copyHeaders()));
		}

		private boolean checkDeclarativeMethod(Method method,
				String methodAnnotatedInboundName, String methodAnnotatedOutboundName) {
			int methodArgumentsLength = method.getParameterTypes().length;
			for (int parameterIndex = 0; parameterIndex < methodArgumentsLength; parameterIndex++) {
				MethodParameter methodParameter = MethodParameter.forExecutable(method,
						parameterIndex);
				if (methodParameter.hasParameterAnnotation(Input.class)) {
					String inboundName = (String) AnnotationUtils.getValue(
							methodParameter.getParameterAnnotation(Input.class));
					Assert.isTrue(StringUtils.hasText(inboundName),
							StreamListenerErrorMessages.INVALID_INBOUND_NAME);
					Assert.isTrue(
							isDeclarativeMethodParameter(inboundName, methodParameter),
							StreamListenerErrorMessages.INVALID_DECLARATIVE_METHOD_PARAMETERS);
					return true;
				}
				else if (methodParameter.hasParameterAnnotation(Output.class)) {
					String outboundName = (String) AnnotationUtils.getValue(
							methodParameter.getParameterAnnotation(Output.class));
					Assert.isTrue(StringUtils.hasText(outboundName),
							StreamListenerErrorMessages.INVALID_OUTBOUND_NAME);
					Assert.isTrue(
							isDeclarativeMethodParameter(outboundName, methodParameter),
							StreamListenerErrorMessages.INVALID_DECLARATIVE_METHOD_PARAMETERS);
					return true;
				}
				else if (StringUtils.hasText(methodAnnotatedOutboundName)) {
					return isDeclarativeMethodParameter(methodAnnotatedOutboundName,
							methodParameter);
				}
				else if (StringUtils.hasText(methodAnnotatedInboundName)) {
					return isDeclarativeMethodParameter(methodAnnotatedInboundName,
							methodParameter);
				}
			}
			return false;
		}

		/**
		 * Determines if method parameters signify an imperative or declarative listener
		 * definition. <br>
		 * Imperative - where handler method is invoked on each message by the handler
		 * infrastructure provided by the framework <br>
		 * Declarative - where handler is provided by the method itself. <br>
		 * Declarative method parameter could either be {@link MessageChannel} or any
		 * other Object for which there is a {@link StreamListenerParameterAdapter} (i.e.,
		 * {@link reactor.core.publisher.Flux}). Declarative method is invoked only once
		 * during initialization phase.
		 * @param targetBeanName name of the bean
		 * @param methodParameter method parameter
		 * @return {@code true} when the method parameter is declarative
		 */
		@SuppressWarnings("unchecked")
		private boolean isDeclarativeMethodParameter(String targetBeanName,
				MethodParameter methodParameter) {
			boolean declarative = false;
			if (!methodParameter.getParameterType().isAssignableFrom(Object.class)
					&& this.applicationContext.containsBean(targetBeanName)) {
				declarative = MessageChannel.class
						.isAssignableFrom(methodParameter.getParameterType());
				if (!declarative) {
					Class<?> targetBeanClass = this.applicationContext
							.getType(targetBeanName);
					declarative = this.streamListenerParameterAdapters.stream().anyMatch(
							slpa -> slpa.supports(targetBeanClass, methodParameter));
				}
			}
			return declarative;
		}

	}

}
