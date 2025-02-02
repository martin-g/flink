/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.metrics;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.metrics.CollectingMetricsReporter.MetricGroupAndName;
import org.apache.flink.runtime.metrics.dump.MetricDumpSerialization;
import org.apache.flink.runtime.metrics.dump.MetricQueryService;
import org.apache.flink.runtime.metrics.groups.MetricGroupTest;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.metrics.scope.ScopeFormats;
import org.apache.flink.runtime.metrics.util.TestReporter;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.webmonitor.retriever.MetricQueryServiceGateway;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterators;

import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link MetricRegistryImpl}. */
public class MetricRegistryImplTest extends TestLogger {

    private static final char GLOBAL_DEFAULT_DELIMITER = '.';

    @Test
    public void testIsShutdown() throws Exception {
        MetricRegistryImpl metricRegistry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration());

        Assert.assertFalse(metricRegistry.isShutdown());

        metricRegistry.shutdown().get();

        Assert.assertTrue(metricRegistry.isShutdown());
    }

    /** Reporter that exposes whether open() was called. */
    protected static class TestReporter1 extends TestReporter {
        public static boolean wasOpened = false;

        @Override
        public void open(MetricConfig config) {
            wasOpened = true;
        }
    }

    @Test
    public void testMetricQueryServiceSetup() throws Exception {
        MetricRegistryImpl metricRegistry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration());

        Assert.assertNull(metricRegistry.getMetricQueryServiceGatewayRpcAddress());

        metricRegistry.startQueryService(new TestingRpcService(), new ResourceID("mqs"));

        MetricQueryServiceGateway metricQueryServiceGateway =
                metricRegistry.getMetricQueryServiceGateway();
        Assert.assertNotNull(metricQueryServiceGateway);

        metricRegistry.register(
                new SimpleCounter(),
                "counter",
                UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup());

        boolean metricsSuccessfullyQueried = false;
        for (int x = 0; x < 10; x++) {
            MetricDumpSerialization.MetricSerializationResult metricSerializationResult =
                    metricQueryServiceGateway
                            .queryMetrics(Time.seconds(5))
                            .get(5, TimeUnit.SECONDS);

            if (metricSerializationResult.numCounters == 1) {
                metricsSuccessfullyQueried = true;
            } else {
                Thread.sleep(50);
            }
        }
        Assert.assertTrue(
                "metrics query did not return expected result", metricsSuccessfullyQueried);
    }

    /** Reporter that exposes the {@link MetricConfig} it was given. */
    protected static class TestReporter2 extends TestReporter {
        static MetricConfig mc;

        @Override
        public void open(MetricConfig config) {
            mc = config;
        }
    }

    /**
     * Verifies that reporters implementing the Scheduled interface are regularly called to report
     * the metrics.
     */
    @Test
    public void testReporterScheduling() throws Exception {
        MetricConfig config = new MetricConfig();
        config.setProperty("arg1", "hello");
        config.setProperty(MetricOptions.REPORTER_INTERVAL.key(), "50 MILLISECONDS");

        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration(),
                        Collections.singletonList(
                                ReporterSetup.forReporter("test", config, new TestReporter3())));

        long start = System.currentTimeMillis();

        // only start counting from now on
        TestReporter3.reportCount = 0;

        for (int x = 0; x < 10; x++) {
            Thread.sleep(100);
            int reportCount = TestReporter3.reportCount;
            long curT = System.currentTimeMillis();
            /**
             * Within a given time-frame T only T/500 reports may be triggered due to the interval
             * between reports. This value however does not not take the first triggered report into
             * account (=> +1). Furthermore we have to account for the mis-alignment between reports
             * being triggered and our time measurement (=> +1); for T=200 a total of 4-6 reports
             * may have been triggered depending on whether the end of the interval for the first
             * reports ends before or after T=50.
             */
            long maxAllowedReports = (curT - start) / 50 + 2;
            Assert.assertTrue("Too many reports were triggered.", maxAllowedReports >= reportCount);
        }
        Assert.assertTrue("No report was triggered.", TestReporter3.reportCount > 0);

        registry.shutdown().get();
    }

    @Test
    public void testReporterIntervalParsingErrorFallsBackToDefaultValue() throws Exception {
        MetricConfig config = new MetricConfig();
        // in a prior implementation the time amount was applied even if the time unit was invalid
        // in this case this would imply using 1 SECOND as the interval (seconds is the default)
        config.setProperty(MetricOptions.REPORTER_INTERVAL.key(), "1 UNICORN");

        final ManuallyTriggeredScheduledExecutorService manuallyTriggeredScheduledExecutorService =
                new ManuallyTriggeredScheduledExecutorService();

        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration(),
                        Collections.singletonList(
                                ReporterSetup.forReporter("test", config, new TestReporter3())),
                        manuallyTriggeredScheduledExecutorService);
        try {
            Collection<ScheduledFuture<?>> scheduledTasks =
                    manuallyTriggeredScheduledExecutorService.getActiveScheduledTasks();
            ScheduledFuture<?> reportTask = Iterators.getOnlyElement(scheduledTasks.iterator());
            Assert.assertEquals(
                    MetricOptions.REPORTER_INTERVAL.defaultValue().getSeconds(),
                    reportTask.getDelay(TimeUnit.SECONDS));
        } finally {
            registry.shutdown().get();
        }
    }

    /** Reporter that exposes how often report() was called. */
    protected static class TestReporter3 extends TestReporter implements Scheduled {
        public static int reportCount = 0;

        @Override
        public void report() {
            reportCount++;
        }
    }

    /** Verifies that reporters are notified of added/removed metrics. */
    @Test
    public void testReporterNotifications() throws Exception {
        Configuration config = new Configuration();
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test1."
                        + MetricOptions.REPORTER_CLASS.key(),
                TestReporter6.class.getName());
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test2."
                        + MetricOptions.REPORTER_CLASS.key(),
                TestReporter7.class.getName());

        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration(),
                        Arrays.asList(
                                ReporterSetup.forReporter("test1", new TestReporter6()),
                                ReporterSetup.forReporter("test2", new TestReporter7())));

        TaskManagerMetricGroup root =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "host", new ResourceID("id"));
        root.counter("rootCounter");

        assertTrue(TestReporter6.addedMetric instanceof Counter);
        assertEquals("rootCounter", TestReporter6.addedMetricName);

        assertTrue(TestReporter7.addedMetric instanceof Counter);
        assertEquals("rootCounter", TestReporter7.addedMetricName);

        root.close();

        assertTrue(TestReporter6.removedMetric instanceof Counter);
        assertEquals("rootCounter", TestReporter6.removedMetricName);

        assertTrue(TestReporter7.removedMetric instanceof Counter);
        assertEquals("rootCounter", TestReporter7.removedMetricName);

        registry.shutdown().get();
    }

    /**
     * Reporter that exposes the name and metric instance of the last metric that was added or
     * removed.
     */
    protected static class TestReporter6 extends TestReporter {
        static Metric addedMetric;
        static String addedMetricName;

        static Metric removedMetric;
        static String removedMetricName;

        @Override
        public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
            addedMetric = metric;
            addedMetricName = metricName;
        }

        @Override
        public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
            removedMetric = metric;
            removedMetricName = metricName;
        }
    }

    /**
     * Reporter that exposes the name and metric instance of the last metric that was added or
     * removed.
     */
    protected static class TestReporter7 extends TestReporter {
        static Metric addedMetric;
        static String addedMetricName;

        static Metric removedMetric;
        static String removedMetricName;

        @Override
        public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
            addedMetric = metric;
            addedMetricName = metricName;
        }

        @Override
        public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
            removedMetric = metric;
            removedMetricName = metricName;
        }
    }

    /** Verifies that the scope configuration is properly extracted. */
    @Test
    public void testScopeConfig() {
        Configuration config = new Configuration();

        config.setString(MetricOptions.SCOPE_NAMING_TM, "A");
        config.setString(MetricOptions.SCOPE_NAMING_TM_JOB, "B");
        config.setString(MetricOptions.SCOPE_NAMING_TASK, "C");
        config.setString(MetricOptions.SCOPE_NAMING_OPERATOR, "D");

        ScopeFormats scopeConfig = ScopeFormats.fromConfig(config);

        assertEquals("A", scopeConfig.getTaskManagerFormat().format());
        assertEquals("B", scopeConfig.getTaskManagerJobFormat().format());
        assertEquals("C", scopeConfig.getTaskFormat().format());
        assertEquals("D", scopeConfig.getOperatorFormat().format());
    }

    @Test
    public void testConfigurableDelimiter() throws Exception {
        Configuration config = new Configuration();
        config.setString(MetricOptions.SCOPE_DELIMITER, "_");
        config.setString(MetricOptions.SCOPE_NAMING_TM, "A.B.C.D.E");

        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.fromConfiguration(config),
                        ReporterSetup.fromConfiguration(config, null));

        TaskManagerMetricGroup tmGroup =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "host", new ResourceID("id"));
        assertEquals("A_B_C_D_E_name", tmGroup.getMetricIdentifier("name"));

        registry.shutdown().get();
    }

    @Test
    public void testConfigurableDelimiterForReporters() throws Exception {
        MetricConfig config1 = new MetricConfig();
        config1.setProperty(MetricOptions.REPORTER_SCOPE_DELIMITER.key(), "_");

        MetricConfig config2 = new MetricConfig();
        config2.setProperty(MetricOptions.REPORTER_SCOPE_DELIMITER.key(), "-");

        MetricConfig config3 = new MetricConfig();
        config3.setProperty(MetricOptions.REPORTER_SCOPE_DELIMITER.key(), "AA");

        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration(),
                        Arrays.asList(
                                ReporterSetup.forReporter("test1", config1, new TestReporter()),
                                ReporterSetup.forReporter("test2", config2, new TestReporter()),
                                ReporterSetup.forReporter("test3", config3, new TestReporter())));

        assertEquals(GLOBAL_DEFAULT_DELIMITER, registry.getDelimiter());
        assertEquals('_', registry.getDelimiter(0));
        assertEquals('-', registry.getDelimiter(1));
        assertEquals(GLOBAL_DEFAULT_DELIMITER, registry.getDelimiter(2));
        assertEquals(GLOBAL_DEFAULT_DELIMITER, registry.getDelimiter(3));
        assertEquals(GLOBAL_DEFAULT_DELIMITER, registry.getDelimiter(-1));

        registry.shutdown().get();
    }

    @Test
    public void testConfigurableDelimiterForReportersInGroup() throws Exception {
        String name = "C";
        MetricConfig config1 = new MetricConfig();
        config1.setProperty(MetricOptions.REPORTER_SCOPE_DELIMITER.key(), "_");

        MetricConfig config2 = new MetricConfig();
        config2.setProperty(MetricOptions.REPORTER_SCOPE_DELIMITER.key(), "-");

        MetricConfig config3 = new MetricConfig();
        config3.setProperty(MetricOptions.REPORTER_SCOPE_DELIMITER.key(), "AA");

        Configuration config = new Configuration();
        config.setString(MetricOptions.SCOPE_NAMING_TM, "A.B");

        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test1."
                        + MetricOptions.REPORTER_SCOPE_DELIMITER.key(),
                "_");
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test1."
                        + MetricOptions.REPORTER_CLASS.key(),
                CollectingMetricsReporter.class.getName());
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test2."
                        + MetricOptions.REPORTER_SCOPE_DELIMITER.key(),
                "-");
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test2."
                        + MetricOptions.REPORTER_CLASS.key(),
                CollectingMetricsReporter.class.getName());
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test3."
                        + MetricOptions.REPORTER_SCOPE_DELIMITER.key(),
                "AA");
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test3."
                        + MetricOptions.REPORTER_CLASS.key(),
                CollectingMetricsReporter.class.getName());
        config.setString(
                ConfigConstants.METRICS_REPORTER_PREFIX
                        + "test4."
                        + MetricOptions.REPORTER_CLASS.key(),
                CollectingMetricsReporter.class.getName());

        List<ReporterSetup> reporterConfigurations =
                Arrays.asList(
                        ReporterSetup.forReporter(
                                "test1", config1, new CollectingMetricsReporter()),
                        ReporterSetup.forReporter(
                                "test2", config2, new CollectingMetricsReporter()),
                        ReporterSetup.forReporter(
                                "test3", config3, new CollectingMetricsReporter()),
                        ReporterSetup.forReporter("test4", new CollectingMetricsReporter()));
        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.fromConfiguration(config), reporterConfigurations);

        TaskManagerMetricGroup group =
                TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        registry, "host", new ResourceID("id"));
        group.counter(name);
        group.close();
        registry.shutdown().get();

        for (ReporterSetup cfg : reporterConfigurations) {
            String delimiter =
                    cfg.getConfiguration()
                            .getProperty(MetricOptions.REPORTER_SCOPE_DELIMITER.key());
            if (delimiter == null || delimiter.equals("AA")) {
                // test3 reporter: 'AA' - not correct
                // for test4 reporter use global delimiter
                delimiter = String.valueOf(GLOBAL_DEFAULT_DELIMITER);
            }
            String expected =
                    (config.get(MetricOptions.SCOPE_NAMING_TM) + ".C").replaceAll("\\.", delimiter);
            CollectingMetricsReporter reporter = (CollectingMetricsReporter) cfg.getReporter();

            for (MetricGroupAndName groupAndName :
                    Arrays.asList(reporter.findAdded(name), reporter.findRemoved(name))) {
                assertEquals(expected, groupAndName.group.getMetricIdentifier(name));
                assertEquals(expected, groupAndName.group.getMetricIdentifier(name, reporter));
            }
        }
    }

    /** Tests that the query actor will be stopped when the MetricRegistry is shut down. */
    @Test
    public void testQueryActorShutdown() throws Exception {
        final Duration timeout = Duration.ofSeconds(10L);

        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration());

        final RpcService rpcService = new TestingRpcService();

        registry.startQueryService(rpcService, null);

        MetricQueryService queryService = checkNotNull(registry.getQueryService());

        registry.shutdown().get();

        queryService.getTerminationFuture().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Test
    public void testExceptionIsolation() throws Exception {
        MetricRegistryImpl registry =
                new MetricRegistryImpl(
                        MetricRegistryTestUtils.defaultMetricRegistryConfiguration(),
                        Arrays.asList(
                                ReporterSetup.forReporter("test1", new FailingReporter()),
                                ReporterSetup.forReporter("test2", new TestReporter7())));

        Counter metric = new SimpleCounter();
        registry.register(
                metric, "counter", new MetricGroupTest.DummyAbstractMetricGroup(registry));

        assertEquals(metric, TestReporter7.addedMetric);
        assertEquals("counter", TestReporter7.addedMetricName);

        registry.unregister(
                metric, "counter", new MetricGroupTest.DummyAbstractMetricGroup(registry));

        assertEquals(metric, TestReporter7.removedMetric);
        assertEquals("counter", TestReporter7.removedMetricName);

        registry.shutdown().get();
    }

    /** Reporter that throws an exception when it is notified of an added or removed metric. */
    protected static class FailingReporter extends TestReporter {
        @Override
        public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
            throw new RuntimeException();
        }

        @Override
        public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
            throw new RuntimeException();
        }
    }
}
