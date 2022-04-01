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

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.decorators.schedulers.CustomizedScheduler;
import org.apache.flink.kubernetes.kubeclient.decorators.schedulers.KubernetesCustomizedScheduler;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;

import io.fabric8.kubernetes.api.model.HasMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class CustomizedConfDecorator extends AbstractKubernetesStepDecorator {

    private final AbstractKubernetesParameters kubernetesComponentConf;
    private final Configuration flinkConfig;
    private final String DEFAULT_SCHEDULER_NAME = "default-scheduler";
    private CustomizedScheduler customizedScheduler = null;

    public CustomizedConfDecorator(AbstractKubernetesParameters kubernetesComponentConf) {
        this.kubernetesComponentConf = checkNotNull(kubernetesComponentConf);
        this.flinkConfig = checkNotNull(kubernetesComponentConf.getFlinkConfiguration());
        this.customizedScheduler = checkHasCustomizedScheduler(kubernetesComponentConf);
    }

    private KubernetesCustomizedScheduler checkHasCustomizedScheduler(
            AbstractKubernetesParameters kubernetesComponentConf) {
        String configuredSchdulerName = kubernetesComponentConf.getPodSchedulerName();
        if (configuredSchdulerName == null
                || configuredSchdulerName.equals(DEFAULT_SCHEDULER_NAME)) {
            return null;
        }

        KubernetesCustomizedScheduler kubernetesCustomizedScheduler =
                new KubernetesCustomizedScheduler(this.kubernetesComponentConf, this.flinkConfig);
        return kubernetesCustomizedScheduler.getSchedulerByName(configuredSchdulerName);
    }

    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        if (this.customizedScheduler == null) {
            return flinkPod;
        }

        List<Map<String, String>> attrs = new ArrayList<>();

        // annotations
        // Map<String, String> customerizedAnnotations =
        // kubernetesComponentConf.getCustomerizedAnnotations();
        //        for (Map.Entry<String, String> entry : customerizedAnnotations.entrySet()) {
        //            attrs.put(entry.getKey(), entry.getValue());
        //        }
        attrs.add(kubernetesComponentConf.getCustomizedAnnotations());
        // customized config
        Map<String, String> configs = kubernetesComponentConf.getPodCustomizedConfig();
        //        for (Map.Entry<String, String> entry : configs.entrySet()) {
        //            attrs.put(entry.getKey(), entry.getValue());
        //        }
        attrs.add(kubernetesComponentConf.getPodCustomizedConfig());

        this.customizedScheduler.settingPropertyIntoScheduler(attrs);
        return this.customizedScheduler.mergePropertyIntoPod(flinkPod);
    }

    @Override
    public List<HasMetadata> buildAccompanyingKubernetesResources() throws IOException {
        HasMetadata hasMetadata = this.customizedScheduler.prepareRequestResources();
        if (hasMetadata != null) {
            return Collections.singletonList(hasMetadata);
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public List<HasMetadata> buildPreAccompanyingKubernetesResources() {
        HasMetadata hasMetadata = this.customizedScheduler.prepareRequestResources();
        if (hasMetadata != null) {
            return Collections.singletonList(hasMetadata);
        } else {
            return Collections.emptyList();
        }
    }
}
