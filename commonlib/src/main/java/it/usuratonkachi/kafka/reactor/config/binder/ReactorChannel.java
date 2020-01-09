package it.usuratonkachi.kafka.reactor.config.binder;

import java.lang.annotation.*;

@Target({ ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface ReactorChannel {

    String value();
    Class<?> messageType();

}
