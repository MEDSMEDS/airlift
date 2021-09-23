/*
 * Copyright 2010 Proofpoint, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.bootstrap;

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableSortedMap;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Stage;
import com.google.inject.spi.Message;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.ConfigurationInspector;
import io.airlift.configuration.ConfigurationInspector.ConfigAttribute;
import io.airlift.configuration.ConfigurationInspector.ConfigRecord;
import io.airlift.configuration.ConfigurationModule;
import io.airlift.configuration.ValidationErrorModule;
import io.airlift.configuration.WarningsMonitor;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.log.LoggingConfiguration;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.airlift.configuration.ConfigurationLoader.getSystemProperties;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static java.lang.String.format;

/**
 * Entry point for an application built using the platform codebase.
 * <p>
 * This class will:
 * <ul>
 * <li>load, validate and bind configurations</li>
 * <li>initialize logging</li>
 * <li>set up bootstrap management</li>
 * <li>create an Guice injector</li>
 * </ul>
 */
public class Bootstrap
{
    private static final Pattern ENV_PATTERN = Pattern.compile("\\$\\{ENV:([a-zA-Z][a-zA-Z0-9_]*)}");

    private final Logger log = Logger.get("Bootstrap");
    private final List<Module> modules;

    private Map<String, String> requiredConfigurationProperties;
    private Map<String, String> optionalConfigurationProperties;
    private boolean initializeLogging = true;
    private boolean quiet;
    private boolean strictConfig;
    private boolean requireExplicitBindings = true;

    private boolean initialized;

    public Bootstrap(Module... modules)
    {
        this(ImmutableList.copyOf(modules));
    }

    public Bootstrap(Iterable<? extends Module> modules)
    {
        this.modules = ImmutableList.copyOf(modules);
    }

    @Beta
    public Bootstrap setRequiredConfigurationProperty(String key, String value)
    {
        if (this.requiredConfigurationProperties == null) {
            this.requiredConfigurationProperties = new TreeMap<>();
        }
        this.requiredConfigurationProperties.put(key, value);
        return this;
    }

    @Beta
    public Bootstrap setRequiredConfigurationProperties(Map<String, String> requiredConfigurationProperties)
    {
        if (this.requiredConfigurationProperties == null) {
            this.requiredConfigurationProperties = new TreeMap<>();
        }
        this.requiredConfigurationProperties.putAll(requiredConfigurationProperties);
        return this;
    }

    @Beta
    public Bootstrap setOptionalConfigurationProperty(String key, String value)
    {
        if (this.optionalConfigurationProperties == null) {
            this.optionalConfigurationProperties = new TreeMap<>();
        }
        this.optionalConfigurationProperties.put(key, value);
        return this;
    }

    @Beta
    public Bootstrap setOptionalConfigurationProperties(Map<String, String> optionalConfigurationProperties)
    {
        if (this.optionalConfigurationProperties == null) {
            this.optionalConfigurationProperties = new TreeMap<>();
        }
        this.optionalConfigurationProperties.putAll(optionalConfigurationProperties);
        return this;
    }

    @Beta
    public Bootstrap doNotInitializeLogging()
    {
        this.initializeLogging = false;
        return this;
    }

    public Bootstrap quiet()
    {
        this.quiet = true;
        return this;
    }

    public Bootstrap strictConfig()///required config必须全部注册
    {
        this.strictConfig = true;
        return this;
    }

    @SuppressWarnings("unused")
    public Bootstrap requireExplicitBindings(boolean requireExplicitBindings)
    {
        this.requireExplicitBindings = requireExplicitBindings;
        return this;
    }
/// 1、requiredProperties赋值
/// 2、获取系统属性
/// 3、替换环境变量
/// 4、创建配置工厂
/// 5、是否初始化配置模块
/// 6、configurationProvider搞点事情，设置变量
/// 7、测试configurationProvider是否创建成功。副作用：此时有些变量已经被修改。可以用来告警没用到的用户配置。
/// 8、打印所有配置。
/// 9、将所有模型注入。启动lifeCycleManager
//java -cp /data/avonxu/presto/presto-server-0.242.1-SNAPSHOT/lib/* -server -Xmx169G -XX:+UseG1GC -XX:G1HeapRegionSize=32M -XX:+UseGCOverheadLimit -XX:+ExplicitGCInvokesConcurrent -XX:+HeapDumpOnOutOfMemoryError
//-Djdk.attach.allowAttachSelf=true -XX:+ExitOnOutOfMemoryError
//-Dlog4j.configurationFile=/data/avonxu/presto-server-0.239/1.log
//-Dlog.output-file=/var/presto239/data/var/log/server.log
//-Dnode.data-dir=/var/presto239/data
//-Dnode.id=ffffffff-ffff-ffff-ffff-ffffffffffff
//-Dnode.environment=production_242
//-Dlog.enable-console=false
//-Dlog.levels-file=/data/avonxu/presto/presto-server-0.242.1-SNAPSHOT/etc/log.properties
//-Dconfig=/data/avonxu/presto/presto-server-0.242.1-SNAPSHOT/etc/config.properties
//com.facebook.presto.server.PrestoServer

/// 通过系统变量读取config.properties来加载配置。java -Dkey=value，来把配置放入系统配置
    public Injector initialize()
    {
        Preconditions.checkState(!initialized, "Already initialized");
        initialized = true;

        Logging logging = null;
        if (initializeLogging) {
            logging = Logging.initialize();
        }

        Thread.currentThread().setUncaughtExceptionHandler((thread, throwable) -> log.error(throwable, "Uncaught exception in thread %s", thread.getName()));

        List<Message> messages = new ArrayList<>();

        Map<String, String> requiredProperties;
        ConfigurationFactory configurationFactory;
        if (requiredConfigurationProperties == null) {///一般是调用setRequiredConfigurationProperty设置。
            // initialize configuration
            log.info("Loading configuration");

            requiredProperties = Collections.emptyMap();
            String configFile = System.getProperty("config");
            if (configFile != null) {
                try {
                    requiredProperties = loadPropertiesFrom(configFile); /// 重要：配置从这里读取
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
        else {
            requiredProperties = requiredConfigurationProperties;
        }
        Map<String, String> unusedProperties = new TreeMap<>(requiredProperties);/// 在strictConfig下，计算是否有没用到的kv

        // combine property sources
        Map<String, String> properties = new HashMap<>();
        if (optionalConfigurationProperties != null) {
            properties.putAll(optionalConfigurationProperties);
        }
        properties.putAll(requiredProperties);
        properties.putAll(getSystemProperties());

        // replace environment variables in property values
        properties = replaceEnvironmentVariables(properties, System.getenv(), (key, error) -> {
            unusedProperties.remove(key); /// 出错以后移除key
            messages.add(new Message(error)); ///
        });

        // create configuration factory
        properties = ImmutableSortedMap.copyOf(properties);

        configurationFactory = new ConfigurationFactory(properties, log::warn);

        // initialize logging
        if (logging != null) {
            log.info("Initializing logging");
            LoggingConfiguration configuration = configurationFactory.build(LoggingConfiguration.class);
            logging.configure(configuration);
        }

        // Register configuration classes defined in the modules
        configurationFactory.registerConfigurationClasses(modules);///此时还未装载

        // Validate configuration classes
        messages.addAll(configurationFactory.validateRegisteredConfigurationProvider());///只是测试

        // at this point all config file properties should be used
        // so we can calculate the unused properties
        unusedProperties.keySet().removeAll(configurationFactory.getUsedProperties());///检查没用到的配置项

        if (strictConfig) {
            for (String key : unusedProperties.keySet()) {
                messages.add(new Message(format("Configuration property '%s' was not used", key)));
            }
        }

        // Log effective configuration 8
        if (!quiet) {
            logConfiguration(configurationFactory, unusedProperties);
        }

        // system modules
        Builder<Module> moduleList = ImmutableList.builder();
        moduleList.add(new LifeCycleModule());
        moduleList.add(new ConfigurationModule(configurationFactory));/// 装载配置工厂
        if (!messages.isEmpty()) {
            moduleList.add(new ValidationErrorModule(messages));
        }
        moduleList.add(binder -> binder.bind(WarningsMonitor.class).toInstance(log::warn));

        // disable broken Guice "features"
        moduleList.add(Binder::disableCircularProxies);
        if (requireExplicitBindings) {
            moduleList.add(Binder::requireExplicitBindings);
        }

        moduleList.addAll(modules);

        // create the injector 9
        Injector injector = Guice.createInjector(Stage.PRODUCTION, moduleList.build());

        // Create the life-cycle manager
        LifeCycleManager lifeCycleManager = injector.getInstance(LifeCycleManager.class);

        // Start services 10
        if (lifeCycleManager.size() > 0) {
            lifeCycleManager.start();
        }

        return injector;
    }

    private void logConfiguration(ConfigurationFactory configurationFactory, Map<String, String> unusedProperties)
    {
        ColumnPrinter columnPrinter = makePrinterForConfiguration(configurationFactory);

        try (PrintWriter out = new PrintWriter(new LoggingWriter(log))) {
            columnPrinter.print(out);
        }

        // Warn about unused properties
        if (!unusedProperties.isEmpty()) {
            log.warn("UNUSED PROPERTIES");
            for (String unusedProperty : unusedProperties.keySet()) {
                log.warn("%s", unusedProperty);
            }
            log.warn("");
        }
    }

    private static ColumnPrinter makePrinterForConfiguration(ConfigurationFactory configurationFactory)
    {
        ConfigurationInspector configurationInspector = new ConfigurationInspector();

        ColumnPrinter columnPrinter = new ColumnPrinter(
                "PROPERTY", "DEFAULT", "RUNTIME", "DESCRIPTION");

        for (ConfigRecord<?> record : configurationInspector.inspect(configurationFactory)) {
            for (ConfigAttribute attribute : record.getAttributes()) {
                columnPrinter.addValues(
                        attribute.getPropertyName(),
                        attribute.getDefaultValue(),
                        attribute.getCurrentValue(),
                        attribute.getDescription());
            }
        }
        return columnPrinter;
    }

    @VisibleForTesting
    static Map<String, String> replaceEnvironmentVariables(
            Map<String, String> properties,
            Map<String, String> environment,
            BiConsumer<String, String> onError)
    {
        Map<String, String> replaced = new HashMap<>();
        properties.forEach((propertyKey, propertyValue) -> {
            Matcher matcher = ENV_PATTERN.matcher(propertyValue);
            if (!matcher.matches()) {
                replaced.put(propertyKey, propertyValue);
                return;
            }
            String envName = matcher.group(1);
            String envValue = environment.get(envName);
            if (envValue == null) {
                onError.accept(propertyKey, format("Configuration property '%s' references unset environment variable '%s'", propertyKey, envName));
                return;
            }
            replaced.put(propertyKey, envValue);
        });
        return replaced;
    }
}
