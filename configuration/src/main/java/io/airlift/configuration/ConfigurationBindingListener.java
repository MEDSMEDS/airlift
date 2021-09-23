package io.airlift.configuration;

public interface ConfigurationBindingListener ///没看到实现
{
    void configurationBound(ConfigurationBinding<?> configurationBinding, ConfigBinder configBinder);
}
