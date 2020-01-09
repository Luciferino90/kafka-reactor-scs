/*
 * Copyright 2018-2019 the original author or authors.
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

package it.usuratonkachi.kafka.reactor.config.annotation;

import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.binding.StreamListenerAnnotationBeanPostProcessor;
import org.springframework.cloud.stream.binding.StreamListenerErrorMessages;
import org.springframework.cloud.stream.binding.StreamListenerParameterAdapter;
import org.springframework.cloud.stream.binding.StreamListenerResultAdapter;
import org.springframework.context.ApplicationContext;
import org.springframework.core.MethodParameter;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.lang.reflect.Method;

/**
 * Orchestrator used for invoking the {@link ReactorStreamListener} setup method.
 *
 * By default {@link StreamListenerAnnotationBeanPostProcessor} will use an internal
 * implementation of this interface to invoke {@link StreamListenerParameterAdapter}s and
 * {@link StreamListenerResultAdapter}s or handler mappings on the method annotated with
 * {@link ReactorStreamListener}.
 *
 * By providing a different implementation of this interface and registering it as a
 * Spring Bean in the context, one can override the default invocation strategies used by
 * the {@link StreamListenerAnnotationBeanPostProcessor}. A typical usecase for such
 * overriding can happen when a downstream
 * {@link org.springframework.cloud.stream.binder.Binder} implementation wants to change
 * the way in which any of the default ReactorStreamListener handling needs to be changed in a
 * custom manner.
 *
 * When beans of this interface are present in the context, they get priority in the
 * {@link StreamListenerAnnotationBeanPostProcessor} before falling back to the default
 * implementation.
 *
 * @see org.springframework.cloud.stream.binding.StreamListenerSetupMethodOrchestrator
 * @see ReactorStreamListener
 * @see StreamListenerAnnotationBeanPostProcessor
 */
public interface ReactorStreamListenerSetupMethodOrchestrator {

	/**
	 * Checks the method annotated with {@link ReactorStreamListener} to see if this
	 * implementation can successfully orchestrate this method.
	 * @param method annotated with {@link ReactorStreamListener}
	 * @return true if this implementation can orchestrate this method, false otherwise
	 */
	boolean supports(Method method);

	/**
	 * Method that allows custom orchestration on the {@link ReactorStreamListener} setup method.
	 * @param ReactorStreamListener reference to the {@link ReactorStreamListener} annotation on the
	 * method
	 * @param method annotated with {@link ReactorStreamListener}
	 * @param bean that contains the ReactorStreamListener method
	 *
	 */
	void orchestrateStreamListenerSetupMethod(ReactorStreamListener ReactorStreamListener,
                                              Method method, Object bean);

	/**
	 * Default implementation for adapting each of the incoming method arguments using an
	 * available {@link StreamListenerParameterAdapter} and provide the adapted collection
	 * of arguments back to the caller.
	 * @param method annotated with {@link ReactorStreamListener}
	 * @param inboundName inbound binding
	 * @param applicationContext spring application context
	 * @param ReactorStreamListenerParameterAdapters used for adapting the method arguments
	 * @return adapted incoming arguments
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	default Object[] adaptAndRetrieveInboundArguments(Method method, String inboundName,
                                                      ApplicationContext applicationContext,
                                                      StreamListenerParameterAdapter... ReactorStreamListenerParameterAdapters) {
		Object[] arguments = new Object[method.getParameterTypes().length];
		for (int parameterIndex = 0; parameterIndex < arguments.length; parameterIndex++) {
			MethodParameter methodParameter = MethodParameter.forExecutable(method,
					parameterIndex);
			Class<?> parameterType = methodParameter.getParameterType();
			Object targetReferenceValue = null;
			if (methodParameter.hasParameterAnnotation(Input.class)) {
				targetReferenceValue = AnnotationUtils
						.getValue(methodParameter.getParameterAnnotation(Input.class));
			}
			else if (methodParameter.hasParameterAnnotation(Output.class)) {
				targetReferenceValue = AnnotationUtils
						.getValue(methodParameter.getParameterAnnotation(Output.class));
			}
			else if (arguments.length == 1 && StringUtils.hasText(inboundName)) {
				targetReferenceValue = inboundName;
			}
			if (targetReferenceValue != null) {
				Assert.isInstanceOf(String.class, targetReferenceValue,
						"Annotation value must be a String");
				Object targetBean = applicationContext
						.getBean((String) targetReferenceValue);
				// Iterate existing parameter adapters first
				for (StreamListenerParameterAdapter streamListenerParameterAdapter : ReactorStreamListenerParameterAdapters) {
					if (streamListenerParameterAdapter.supports(targetBean.getClass(),
							methodParameter)) {
						arguments[parameterIndex] = streamListenerParameterAdapter
								.adapt(targetBean, methodParameter);
						break;
					}
				}
				if (arguments[parameterIndex] == null
						&& parameterType.isAssignableFrom(targetBean.getClass())) {
					arguments[parameterIndex] = targetBean;
				}
				Assert.notNull(arguments[parameterIndex],
						"Cannot convert argument " + parameterIndex + " of " + method
								+ "from " + targetBean.getClass() + " to "
								+ parameterType);
			}
			else {
				throw new IllegalStateException(
						StreamListenerErrorMessages.INVALID_DECLARATIVE_METHOD_PARAMETERS);
			}
		}
		return arguments;
	}

}