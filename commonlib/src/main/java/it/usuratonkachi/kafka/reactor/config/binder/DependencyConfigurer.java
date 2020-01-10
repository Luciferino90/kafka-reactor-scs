package it.usuratonkachi.kafka.reactor.config.binder;

import it.usuratonkachi.kafka.reactor.config.annotation.input.ReactorStreamListener;
import it.usuratonkachi.kafka.reactor.config.annotation.output.ReactorMessageChannel;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.stereotype.Component;
import reactor.util.function.Tuples;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Component
public class DependencyConfigurer implements BeanFactoryPostProcessor {

    @Override
    public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) {
        Arrays.stream(beanFactory.getBeanDefinitionNames())
                .map(slaveBeanName -> {
                    try {
                        Class<?> clazz = findClassByBeanName(beanFactory, slaveBeanName);
                        if (clazz == null) return Tuples.of(slaveBeanName, new ArrayList<String>());
                        List<String> methodChannels = Arrays.stream(clazz.getMethods()).filter(method -> method.isAnnotationPresent(ReactorStreamListener.class)).map(method -> method.getAnnotation(ReactorStreamListener.class).value()).collect(Collectors.toList());
                        List<String> fieldChannels = Arrays.stream(clazz.getDeclaredFields()).filter(field -> field.isAnnotationPresent(ReactorMessageChannel.class)).map(field -> field.getAnnotation(ReactorMessageChannel.class).value()).collect(Collectors.toList());
                        methodChannels.addAll(fieldChannels);
                        return Tuples.of(slaveBeanName, methodChannels);
                    } catch (Exception ex) {
                        // Fault tolerant
                        return Tuples.of(slaveBeanName, new ArrayList<String>());
                    }
                })
                .filter(slaveBeanNameKafkaChannels -> !slaveBeanNameKafkaChannels.getT2().isEmpty())
                .forEach(slaveBeanNameKafkaChannels -> innerCheck(beanFactory, slaveBeanNameKafkaChannels.getT1(), slaveBeanNameKafkaChannels.getT2()));
    }

    private void innerCheck(ConfigurableListableBeanFactory beanFactory, String slaveBeanName, List<String> dependentChannels){
        Arrays.stream(beanFactory.getBeanNamesForAnnotation(ReactorChannelBinder.class))
                .forEach(masterBeanName -> {
                    List<String> masterBeanChannels = Arrays.stream(findClassByBeanName(beanFactory, masterBeanName).getDeclaredFields()).filter(field -> field.isAnnotationPresent(ReactorChannel.class)).map(field -> field.getAnnotation(ReactorChannel.class).value()).collect(Collectors.toList());
                    if (dependentChannels.stream().anyMatch(masterBeanChannels::contains))
                        beanFactory.getBeanDefinition(slaveBeanName).setDependsOn(masterBeanName);
                });
    }

    private Class<?> findClassByBeanName(ConfigurableListableBeanFactory beanFactory, String beanName) {
        Optional<String> typeName = Optional.ofNullable(beanFactory.getBeanDefinition(beanName).getBeanClassName());
        if (typeName.isEmpty()) return null;
        Optional<Class<?>> clazzOptional = Optional.ofNullable(findClass(typeName.get()));
        return clazzOptional.orElse(null);
    }

    private Class<?> findClass(String typeName){
        try {
            return Class.forName(typeName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (NoSuchBeanDefinitionException e) {
            // Fault tolerant
            return null;
        }
    }

}
