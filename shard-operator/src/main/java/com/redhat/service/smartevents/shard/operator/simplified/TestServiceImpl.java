package com.redhat.service.smartevents.shard.operator.simplified;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;

@ApplicationScoped
public class TestServiceImpl implements TestService {

    public static final String NAMESPACE_NAME = "mycr-namespace";

    @Inject
    KubernetesClient kubernetesClient;

    @Override
    public void create(String name) {
        MyCR myCR = new MyCR();
        ObjectMeta meta = new ObjectMetaBuilder()
                .withName(name)
                .withNamespace(TestServiceImpl.NAMESPACE_NAME)
                .build();
        myCR.setMetadata(meta);
        myCR.setSpec(new MyCRSpec());
        myCR.setStatus(new MyCRStatus());

        kubernetesClient
                .resources(MyCR.class)
                .inNamespace(NAMESPACE_NAME)
                .createOrReplace(myCR);
    }

    @Override
    public void delete(String name) {
        kubernetesClient
                .resources(MyCR.class)
                .inNamespace(NAMESPACE_NAME)
                .withName(name)
                .delete();
    }

    @Override
    public void deleteByNamespace() {
        kubernetesClient
                .namespaces()
                .withName(NAMESPACE_NAME)
                .delete();
    }
}
