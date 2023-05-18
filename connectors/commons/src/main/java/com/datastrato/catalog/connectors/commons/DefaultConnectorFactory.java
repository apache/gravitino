package com.datastrato.catalog.connectors.commons;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common connector factory with repeatable functionality.
 */
@Getter
public class DefaultConnectorFactory implements ConnectorFactory {
    private static final Logger log = LoggerFactory.getLogger(DefaultConnectorFactory.class);

    private final Injector injector;

    public DefaultConnectorFactory(final Iterable<? extends Module> modules) {
        this.injector = Guice.createInjector(modules);
    }

    @Override
    public void stop() {
    }
}
