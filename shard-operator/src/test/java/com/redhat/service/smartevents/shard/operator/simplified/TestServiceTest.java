package com.redhat.service.smartevents.shard.operator.simplified;

import java.time.Duration;

import javax.inject.Inject;

import org.junit.jupiter.api.Test;

import com.redhat.service.smartevents.test.resource.KeycloakResource;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.kubernetes.client.WithOpenShiftTestServer;

import static com.redhat.service.smartevents.shard.operator.utils.AwaitilityUtil.await;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@QuarkusTest
@WithOpenShiftTestServer
@QuarkusTestResource(value = KeycloakResource.class, restrictToAnnotatedClass = true)
public class TestServiceTest {

    @Inject
    TestService testService;

    @Inject
    KubernetesClient kubernetesClient;

    @Test
    public void testDeletion() {
        String name = "myCR-name";
        testService.create(name);
        waitUntilMyCRExists(name);

        testService.delete(name);
        waitUntilMyCRDoesntExist(name);
    }

    @Test
    public void testDeletionByNamespace() {
        String name = "myCR-name-byNamespace";
        testService.create(name);
        waitUntilMyCRExists(name);

        testService.deleteByNamespace();
        waitUntilMyCRDoesntExist(name);
    }

    private MyCR fetchMyCR(String name) {
        return kubernetesClient
                .resources(MyCR.class)
                .inNamespace(TestServiceImpl.NAMESPACE_NAME)
                .withName(name)
                .get();
    }

    private void waitUntilMyCRExists(String name) {
        await(Duration.ofSeconds(5),
                Duration.ofSeconds(1),
                () -> {
                    MyCR myCR = fetchMyCR(name);
                    assertThat(myCR).isNotNull();
                });
    }

    private void waitUntilMyCRDoesntExist(String name) {
        await(Duration.ofSeconds(5),
                Duration.ofSeconds(1),
                () -> {
                    MyCR myCR = fetchMyCR(name);
                    assertThat(myCR).isNull();
                });
    }
}
