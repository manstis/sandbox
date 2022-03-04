package com.redhat.service.bridge.manager;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;
import javax.transaction.Transactional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.openshift.cloud.api.connector.models.Connector;
import com.openshift.cloud.api.connector.models.ConnectorRequest;
import com.openshift.cloud.api.connector.models.ConnectorState;
import com.openshift.cloud.api.connector.models.ConnectorStatusStatus;
import com.openshift.cloud.api.kas.auth.models.Topic;
import com.redhat.service.bridge.actions.kafkatopic.KafkaTopicAction;
import com.redhat.service.bridge.infra.api.APIConstants;
import com.redhat.service.bridge.infra.exceptions.definitions.platform.InternalPlatformException;
import com.redhat.service.bridge.infra.exceptions.definitions.user.AlreadyExistingItemException;
import com.redhat.service.bridge.infra.exceptions.definitions.user.BridgeLifecycleException;
import com.redhat.service.bridge.infra.exceptions.definitions.user.ItemNotFoundException;
import com.redhat.service.bridge.infra.models.ListResult;
import com.redhat.service.bridge.infra.models.QueryInfo;
import com.redhat.service.bridge.infra.models.actions.BaseAction;
import com.redhat.service.bridge.infra.models.dto.ManagedResourceStatus;
import com.redhat.service.bridge.infra.models.dto.ProcessorDTO;
import com.redhat.service.bridge.infra.models.filters.BaseFilter;
import com.redhat.service.bridge.infra.models.filters.StringEquals;
import com.redhat.service.bridge.infra.models.processors.ProcessorDefinition;
import com.redhat.service.bridge.manager.actions.connectors.SlackAction;
import com.redhat.service.bridge.manager.api.models.requests.ProcessorRequest;
import com.redhat.service.bridge.manager.api.models.responses.ProcessorResponse;
import com.redhat.service.bridge.manager.connectors.ConnectorsApiClient;
import com.redhat.service.bridge.manager.dao.BridgeDAO;
import com.redhat.service.bridge.manager.dao.ConnectorsDAO;
import com.redhat.service.bridge.manager.dao.ProcessorDAO;
import com.redhat.service.bridge.manager.models.Bridge;
import com.redhat.service.bridge.manager.models.ConnectorEntity;
import com.redhat.service.bridge.manager.models.Processor;
import com.redhat.service.bridge.manager.utils.DatabaseManagerUtils;
import com.redhat.service.bridge.manager.utils.Fixtures;
import com.redhat.service.bridge.rhoas.RhoasTopicAccessType;
import com.redhat.service.bridge.test.resource.PostgresResource;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;

import static com.redhat.service.bridge.infra.models.dto.ManagedResourceStatus.DEPROVISION;
import static com.redhat.service.bridge.manager.RhoasServiceImpl.createFailureErrorMessageFor;
import static com.redhat.service.bridge.manager.api.internal.ShardBridgesSyncAPI.SHARD_REQUESTED_STATUS;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@QuarkusTest
@QuarkusTestResource(PostgresResource.class)
public class ProcessorServiceTest {

    @Inject
    BridgeDAO bridgeDAO;

    @Inject
    ProcessorDAO processorDAO;

    @Inject
    ProcessorService processorService;

    @Inject
    ConnectorsDAO connectorsDAO;

    @Inject
    DatabaseManagerUtils databaseManagerUtils;

    @InjectMock
    RhoasService rhoasService;

    @InjectMock
    ConnectorsApiClient connectorsApiClient;

    @BeforeEach
    public void cleanUp() {
        databaseManagerUtils.cleanUpAndInitWithDefaultShard();
        reset(rhoasService);
    }

    private Bridge createPersistBridge(ManagedResourceStatus status) {
        Bridge b = new Bridge();
        b.setName(TestConstants.DEFAULT_BRIDGE_NAME);
        b.setCustomerId(TestConstants.DEFAULT_CUSTOMER_ID);
        b.setStatus(status);
        b.setSubmittedAt(ZonedDateTime.now());
        b.setPublishedAt(ZonedDateTime.now());
        bridgeDAO.persist(b);
        return b;
    }

    private BaseAction createKafkaAction() {
        BaseAction a = new BaseAction();
        a.setType(KafkaTopicAction.TYPE);

        Map<String, String> params = new HashMap<>();
        params.put(KafkaTopicAction.TOPIC_PARAM, TestConstants.DEFAULT_KAFKA_TOPIC);
        a.setParameters(params);
        return a;
    }

    @Test
    public void createProcessor_bridgeNotActive() {
        Bridge b = createPersistBridge(ManagedResourceStatus.PROVISIONING);
        assertThatExceptionOfType(BridgeLifecycleException.class)
                .isThrownBy(() -> processorService.createProcessor(b.getId(), b.getCustomerId(), new ProcessorRequest()));
    }

    @Test
    public void createProcessor_bridgeDoesNotExist() {
        assertThatExceptionOfType(ItemNotFoundException.class)
                .isThrownBy(() -> processorService.createProcessor(TestConstants.DEFAULT_BRIDGE_NAME, TestConstants.DEFAULT_CUSTOMER_ID, new ProcessorRequest()));
    }

    @Test
    public void createProcessor_processorWithSameNameAlreadyExists() {
        Bridge b = createPersistBridge(ManagedResourceStatus.READY);
        ProcessorRequest r = new ProcessorRequest("My Processor", createKafkaAction());

        Processor processor = processorService.createProcessor(b.getId(), b.getCustomerId(), r);
        await().atMost(5, SECONDS).untilAsserted(() -> {
            Processor p = processorDAO.findById(processor.getId());
            assertThat(p).isNotNull();
            assertThat(p.getDependencyStatus()).isEqualTo(ManagedResourceStatus.READY);
        });

        assertThatExceptionOfType(AlreadyExistingItemException.class).isThrownBy(() -> processorService.createProcessor(b.getId(), b.getCustomerId(), r));
    }

    @Test
    public void createProcessor() {
        Bridge b = createPersistBridge(ManagedResourceStatus.READY);
        ProcessorRequest r = new ProcessorRequest("My Processor", null, "{}", createKafkaAction());

        Processor processor = processorService.createProcessor(b.getId(), b.getCustomerId(), r);
        await().atMost(5, SECONDS).untilAsserted(() -> {
            Processor p = processorDAO.findById(processor.getId());
            assertThat(p).isNotNull();
            assertThat(p.getDependencyStatus()).isEqualTo(ManagedResourceStatus.READY);
        });

        assertThat(processor.getBridge().getId()).isEqualTo(b.getId());
        assertThat(processor.getName()).isEqualTo(r.getName());
        assertThat(processor.getStatus()).isEqualTo(ManagedResourceStatus.ACCEPTED);
        assertThat(processor.getSubmittedAt()).isNotNull();
        assertThat(processor.getDefinition()).isNotNull();

        ProcessorDefinition definition = jsonNodeToDefinition(processor.getDefinition());
        assertThat(definition.getTransformationTemplate()).isEqualTo("{}");
    }

    @Test
    @Transactional
    public void getProcessorByStatuses() {
        Bridge b = createPersistBridge(ManagedResourceStatus.READY);

        final Processor processor1 = new Processor();
        processor1.setName("My Processor");
        processor1.setBridge(b);
        processor1.setShardId(TestConstants.SHARD_ID);
        processor1.setStatus(ManagedResourceStatus.ACCEPTED);
        processor1.setSubmittedAt(ZonedDateTime.now());
        processor1.setDefinition(new TextNode("definition"));
        processorDAO.persist(processor1);

        final Processor processor2 = new Processor();
        processor2.setName("My Processor 2");
        processor2.setBridge(b);
        processor2.setShardId(TestConstants.SHARD_ID);
        processor2.setStatus(ManagedResourceStatus.READY);
        processor2.setSubmittedAt(ZonedDateTime.now());
        processor2.setDefinition(new TextNode("definition"));
        processorDAO.persist(processor2);

        final Processor processor3 = new Processor();
        processor3.setName("My Processor 3");
        processor3.setBridge(b);
        processor3.setShardId(TestConstants.SHARD_ID);
        processor3.setStatus(ManagedResourceStatus.DEPROVISION);
        processor3.setSubmittedAt(ZonedDateTime.now());
        processor3.setDefinition(new TextNode("definition"));
        processorDAO.persist(processor3);

        List<Processor> processors = processorService.getProcessorByStatusesAndShardIdWithReadyDependencies(asList(ManagedResourceStatus.ACCEPTED, DEPROVISION), TestConstants.SHARD_ID);
        assertThat(processors.size()).isEqualTo(2);
        processors.forEach((px) -> assertThat(px.getName()).isIn("My Processor", "My Processor 3"));
    }

    @Test
    public void updateProcessorStatus() {
        Bridge b = createPersistBridge(ManagedResourceStatus.READY);
        ProcessorRequest r = new ProcessorRequest("My Processor", createKafkaAction());

        Processor processor = processorService.createProcessor(b.getId(), b.getCustomerId(), r);
        await().atMost(5, SECONDS).untilAsserted(() -> {
            Processor p = processorDAO.findById(processor.getId());
            assertThat(p).isNotNull();
            assertThat(p.getDependencyStatus()).isEqualTo(ManagedResourceStatus.READY);
        });

        ProcessorDTO dto = processorService.toDTO(processor);
        dto.setStatus(ManagedResourceStatus.FAILED);

        Processor updated = processorService.updateProcessorStatus(dto);
        assertThat(updated.getStatus()).isEqualTo(ManagedResourceStatus.FAILED);
    }

    @Test
    public void updateProcessorStatus_bridgeDoesNotExist() {
        ProcessorDTO processor = new ProcessorDTO();
        processor.setBridgeId("foo");

        assertThatExceptionOfType(ItemNotFoundException.class).isThrownBy(() -> processorService.updateProcessorStatus(processor));
    }

    @Test
    public void updateProcessorStatus_processorDoesNotExist() {
        Processor p = new Processor();
        p.setBridge(createPersistBridge(ManagedResourceStatus.READY));
        p.setId("foo");

        ProcessorDTO processor = processorService.toDTO(p);

        assertThatExceptionOfType(ItemNotFoundException.class).isThrownBy(() -> processorService.updateProcessorStatus(processor));
    }

    @Test
    public void getProcessor() {
        Bridge b = createPersistBridge(ManagedResourceStatus.READY);
        ProcessorRequest r = new ProcessorRequest("My Processor", createKafkaAction());

        Processor processor = processorService.createProcessor(b.getId(), b.getCustomerId(), r);
        await().atMost(5, SECONDS).untilAsserted(() -> {
            Processor p = processorDAO.findById(processor.getId());
            assertThat(p).isNotNull();
            assertThat(p.getDependencyStatus()).isEqualTo(ManagedResourceStatus.READY);
        });

        Processor found = processorService.getProcessor(processor.getId(), b.getId(), b.getCustomerId());
        assertThat(found).isNotNull();
        assertThat(found.getId()).isEqualTo(processor.getId());
        assertThat(found.getBridge().getId()).isEqualTo(b.getId());
        assertThat(found.getBridge().getCustomerId()).isEqualTo(b.getCustomerId());
    }

    @Test
    public void getProcessor_bridgeDoesNotExist() {
        Bridge b = createPersistBridge(ManagedResourceStatus.READY);
        ProcessorRequest r = new ProcessorRequest("My Processor", createKafkaAction());

        Processor processor = processorService.createProcessor(b.getId(), b.getCustomerId(), r);
        await().atMost(5, SECONDS).untilAsserted(() -> {
            Processor p = processorDAO.findById(processor.getId());
            assertThat(p).isNotNull();
            assertThat(p.getDependencyStatus()).isEqualTo(ManagedResourceStatus.READY);
        });

        assertThatExceptionOfType(ItemNotFoundException.class).isThrownBy(() -> processorService.getProcessor(processor.getId(), "doesNotExist", b.getCustomerId()));
    }

    @Test
    public void getProcessor_processorDoesNotExist() {
        Bridge b = createPersistBridge(ManagedResourceStatus.READY);
        ProcessorRequest r = new ProcessorRequest("My Processor", createKafkaAction());

        Processor processor = processorService.createProcessor(b.getId(), b.getCustomerId(), r);
        await().atMost(5, SECONDS).untilAsserted(() -> {
            Processor p = processorDAO.findById(processor.getId());
            assertThat(p).isNotNull();
            assertThat(p.getDependencyStatus()).isEqualTo(ManagedResourceStatus.READY);
        });

        assertThatExceptionOfType(ItemNotFoundException.class).isThrownBy(() -> processorService.getProcessor("doesNotExist", b.getId(), b.getCustomerId()));
    }

    @Test
    public void getProcessors() {
        Bridge b = createPersistBridge(ManagedResourceStatus.READY);
        ProcessorRequest r = new ProcessorRequest("My Processor", createKafkaAction());

        Processor processor = processorService.createProcessor(b.getId(), b.getCustomerId(), r);
        await().atMost(5, SECONDS).untilAsserted(() -> {
            Processor p = processorDAO.findById(processor.getId());
            assertThat(p).isNotNull();
            assertThat(p.getDependencyStatus()).isEqualTo(ManagedResourceStatus.READY);
        });

        ListResult<Processor> results = processorService.getProcessors(b.getId(), TestConstants.DEFAULT_CUSTOMER_ID, new QueryInfo(0, 100));
        assertThat(results.getPage()).isZero();
        assertThat(results.getSize()).isEqualTo(1L);
        assertThat(results.getTotal()).isEqualTo(1L);

        assertThat(results.getItems().get(0).getId()).isEqualTo(processor.getId());
    }

    @Test
    public void getProcessors_noProcessorsOnBridge() {
        Bridge b = createPersistBridge(ManagedResourceStatus.READY);
        ListResult<Processor> results = processorService.getProcessors(b.getId(), TestConstants.DEFAULT_CUSTOMER_ID, new QueryInfo(0, 100));
        assertThat(results.getPage()).isZero();
        assertThat(results.getSize()).isZero();
        assertThat(results.getTotal()).isZero();
    }

    @Test
    public void getProcessors_bridgeDoesNotExist() {
        assertThatExceptionOfType(ItemNotFoundException.class).isThrownBy(() -> processorService.getProcessors("doesNotExist", TestConstants.DEFAULT_CUSTOMER_ID, new QueryInfo(0, 100)));
    }

    @Test
    public void deleteProcessor_processorDoesNotExist() {
        Bridge b = createPersistBridge(ManagedResourceStatus.READY);
        assertThatExceptionOfType(ItemNotFoundException.class).isThrownBy(() -> processorService.deleteProcessor(b.getId(), "doesNotExist", b.getCustomerId()));
    }

    @Test
    public void testGetProcessorsCount() {
        Bridge b = createPersistBridge(ManagedResourceStatus.READY);
        ProcessorRequest r = new ProcessorRequest("My Processor", createKafkaAction());

        Processor processor = processorService.createProcessor(b.getId(), b.getCustomerId(), r);
        await().atMost(5, SECONDS).untilAsserted(() -> {
            Processor p = processorDAO.findById(processor.getId());
            assertThat(p).isNotNull();
            assertThat(p.getDependencyStatus()).isEqualTo(ManagedResourceStatus.READY);
        });

        Long result = processorService.getProcessorsCount(b.getId(), TestConstants.DEFAULT_CUSTOMER_ID);
        assertThat(result).isEqualTo(1L);
    }

    @Test
    public void testDeleteProcessor() {
        Bridge b = createPersistBridge(ManagedResourceStatus.READY);
        ProcessorRequest r = new ProcessorRequest("My Processor", createKafkaAction());

        Processor processor = processorService.createProcessor(b.getId(), b.getCustomerId(), r);
        await().atMost(5, SECONDS).untilAsserted(() -> {
            Processor p = processorDAO.findById(processor.getId());
            assertThat(p).isNotNull();
            assertThat(p.getDependencyStatus()).isEqualTo(ManagedResourceStatus.READY);
        });

        processorService.deleteProcessor(b.getId(), processor.getId(), TestConstants.DEFAULT_CUSTOMER_ID);
        await().atMost(5, SECONDS).untilAsserted(() -> {
            Processor p = processorDAO.findById(processor.getId());
            assertThat(p).isNotNull();
            assertThat(p.getDependencyStatus()).isEqualTo(ManagedResourceStatus.DELETED);
            assertThat(p.getStatus()).isEqualTo(ManagedResourceStatus.DEPROVISION);

            assertShardAsksForProcessorToBeDeletedIncludes(processor);
        });
    }

    @Test
    public void testMGDOBR_80() {
        Bridge b = createPersistBridge(ManagedResourceStatus.READY);
        Set<BaseFilter> filters = new HashSet<>();
        filters.add(new StringEquals("name", "myName"));
        filters.add(new StringEquals("surename", "mySurename"));
        ProcessorRequest r = new ProcessorRequest("My Processor", filters, null, createKafkaAction());

        Processor processor = processorService.createProcessor(b.getId(), b.getCustomerId(), r);
        await().atMost(5, SECONDS).untilAsserted(() -> {
            Processor p = processorDAO.findById(processor.getId());
            assertThat(p).isNotNull();
            assertThat(p.getDependencyStatus()).isEqualTo(ManagedResourceStatus.READY);
        });

        assertThat(processorService.getProcessors(b.getId(), TestConstants.DEFAULT_CUSTOMER_ID, new QueryInfo(0, 100)).getSize()).isEqualTo(1);
    }

    @Test
    public void toResponse() {
        Bridge b = Fixtures.createBridge();
        Processor p = Fixtures.createProcessor(b, "foo", ManagedResourceStatus.READY);
        BaseAction action = Fixtures.createKafkaAction();

        ProcessorDefinition definition = new ProcessorDefinition(Collections.emptySet(), "", action);
        p.setDefinition(definitionToJsonNode(definition));

        ProcessorResponse r = processorService.toResponse(p);
        assertThat(r).isNotNull();

        assertThat(r.getHref()).isEqualTo(APIConstants.USER_API_BASE_PATH + b.getId() + "/processors/" + p.getId());
        assertThat(r.getName()).isEqualTo(p.getName());
        assertThat(r.getStatus()).isEqualTo(p.getStatus());
        assertThat(r.getId()).isEqualTo(p.getId());
        assertThat(r.getSubmittedAt()).isEqualTo(p.getSubmittedAt());
        assertThat(r.getPublishedAt()).isEqualTo(p.getPublishedAt());
        assertThat(r.getKind()).isEqualTo("Processor");
        assertThat(r.getTransformationTemplate()).isEmpty();
        assertThat(r.getAction().getType()).isEqualTo(KafkaTopicAction.TYPE);
    }

    private JsonNode definitionToJsonNode(ProcessorDefinition definition) {
        ProcessorServiceImpl processorServiceImpl = (ProcessorServiceImpl) processorService;
        return processorServiceImpl.definitionToJsonNode(definition);
    }

    private ProcessorDefinition jsonNodeToDefinition(JsonNode jsonNode) {
        ProcessorServiceImpl processorServiceImpl = (ProcessorServiceImpl) processorService;
        return processorServiceImpl.jsonNodeToDefinition(jsonNode);
    }

    @Test
    void createConnector() {
        Bridge b = createPersistBridge(ManagedResourceStatus.READY);

        BaseAction slackAction = createSlackAction();
        ProcessorRequest processorRequest = new ProcessorRequest("ManagedConnectorProcessor", slackAction);

        //Emulate successful creation
        Connector externalConnector = stubbedExternalConnector("connectorExternalId");
        ConnectorStatusStatus externalConnectorStatus = new ConnectorStatusStatus();
        externalConnectorStatus.setState(ConnectorState.READY);
        externalConnector.setStatus(externalConnectorStatus);

        //Emulate the connector not being found when first looked up, to force provisioning
        when(connectorsApiClient.getConnector(any())).thenReturn(null, externalConnector);
        when(connectorsApiClient.createConnector(any(ConnectorEntity.class))).thenCallRealMethod();
        when(connectorsApiClient.createConnector(any(ConnectorRequest.class))).thenReturn(externalConnector);
        when(rhoasService.createTopicAndGrantAccessFor(anyString(), any())).thenReturn(new Topic());

        Processor processor = processorService.createProcessor(b.getId(), b.getCustomerId(), processorRequest);

        //There will be 2 re-tries at 5s each. Add 5s to be certain everything completes.
        await().atMost(15000, SECONDS).untilAsserted(() -> {
            ConnectorEntity connector = connectorsDAO.findByProcessorIdAndName(processor.getId(),
                    String.format("OpenBridge-slack_sink_0.1-%s",
                            processor.getId()));

            assertThat(connector).isNotNull();
            assertThat(connector.getError()).isNullOrEmpty();
            assertThat(connector.getStatus()).isEqualTo(ManagedResourceStatus.READY);
        });

        verify(rhoasService, atLeast(1)).createTopicAndGrantAccessFor(anyString(), eq(RhoasTopicAccessType.PRODUCER));

        ArgumentCaptor<ConnectorRequest> connectorCaptor = ArgumentCaptor.forClass(ConnectorRequest.class);
        verify(connectorsApiClient).createConnector(connectorCaptor.capture());
        ConnectorRequest calledConnector = connectorCaptor.getValue();
        assertThat(calledConnector.getKafka()).isNotNull();
    }

    @Test
    public void createConnectorFailureTopic() {
        Bridge b = createPersistBridge(ManagedResourceStatus.READY);

        BaseAction slackAction = createSlackAction();
        ProcessorRequest processorRequest = new ProcessorRequest("ManagedConnectorProcessor", slackAction);

        when(rhoasService.createTopicAndGrantAccessFor(anyString(), any())).thenThrow(
                new InternalPlatformException(createFailureErrorMessageFor("errorTopic"), new RuntimeException("error")));
        when(connectorsApiClient.createConnector(any(ConnectorRequest.class))).thenReturn(new Connector());

        Processor processor = processorService.createProcessor(b.getId(), b.getCustomerId(), processorRequest);
        List<ConnectorEntity> connectors = connectorsDAO.findByProcessorId(processor.getId());
        assertThat(connectors).hasSize(1);

        waitForProcessorAndConnectorToFail(connectors.get(0));

        //TOOD {manstis} Move to above method?
        //There will be 4 re-tries at 5s each. Add 5s to be certain everything completes.
        await().atMost(25000, SECONDS).untilAsserted(() -> {
            Processor p = processorDAO.findById(processor.getId());

            assertThat(p).isNotNull();
            assertThat(p.getStatus()).isEqualTo(ManagedResourceStatus.FAILED);
        });

        verify(rhoasService, atLeast(1)).createTopicAndGrantAccessFor(anyString(), eq(RhoasTopicAccessType.PRODUCER));
        verify(connectorsApiClient, never()).createConnector(any(ConnectorRequest.class));
    }

    @Test
    public void createConnectorFailureConnector() {
        Bridge b = createPersistBridge(ManagedResourceStatus.READY);

        BaseAction slackAction = createSlackAction();
        ProcessorRequest processorRequest = new ProcessorRequest("ManagedConnectorProcessor", slackAction);

        doThrow(new InternalPlatformException(createFailureErrorMessageFor("errorDeletingConnector"), new RuntimeException("error")))
                .when(connectorsApiClient).deleteConnector(anyString());

        Processor processor = processorService.createProcessor(b.getId(), b.getCustomerId(), processorRequest);
        List<ConnectorEntity> connectors = connectorsDAO.findByProcessorId(processor.getId());
        assertThat(connectors).hasSize(1);

        waitForProcessorAndConnectorToFail(connectors.get(0));
    }

    @Test
    public void testDeleteRequestedConnector() {
        Bridge bridge = createPersistBridge(ManagedResourceStatus.READY);
        Processor processor = Fixtures.createProcessor(bridge, "bridgeTestDelete", ManagedResourceStatus.READY);
        ConnectorEntity connector = Fixtures.createConnector(processor,
                "connectorToBeDeleted",
                ManagedResourceStatus.READY,
                "topicName");
        processorDAO.persist(processor);
        connectorsDAO.persist(connector);

        //Emulate successful creation
        Connector externalConnector = new Connector();
        final ConnectorStatusStatus externalConnectorStatus = new ConnectorStatusStatus();
        externalConnectorStatus.setState(ConnectorState.READY);
        externalConnector.setStatus(externalConnectorStatus);

        when(connectorsApiClient.getConnector(any())).thenReturn(externalConnector);
        //Emulate successful deletion
        doAnswer(i -> {
            externalConnectorStatus.setState(ConnectorState.DELETED);
            return null;
        }).when(connectorsApiClient).deleteConnector(any());

        processorService.deleteProcessor(bridge.getId(), processor.getId(), TestConstants.DEFAULT_CUSTOMER_ID);

        reloadAssertProcessorIsInStatus(processor, DEPROVISION);

        waitForConnectorToBeDeleted(connector);

        //There will be 2 re-tries at 5s each. Add 5s to be certain everything completes.
        await().atMost(15, SECONDS).untilAsserted(() -> {
            ConnectorEntity foundConnector = connectorsDAO.findByProcessorIdAndName(processor.getId(), "connectorToBeDeleted");
            assertThat(foundConnector).isNull();

            final Processor processorDeleted = processorService.getProcessor(processor.getId(), bridge.getId(), TestConstants.DEFAULT_CUSTOMER_ID);
            assertThat(processorDeleted.getStatus()).isEqualTo(DEPROVISION);
        });

        verify(rhoasService).deleteTopicAndRevokeAccessFor(eq("topicName"), eq(RhoasTopicAccessType.PRODUCER));
        verify(connectorsApiClient).deleteConnector("connectorExternalId");

        assertShardAsksForProcessorToBeDeletedIncludes(processor);
    }

    @Test
    public void testDeleteConnectorFailingTopic() {
        Bridge bridge = createPersistBridge(ManagedResourceStatus.READY);
        Processor processor = Fixtures.createProcessor(bridge, "bridgeTestDelete", ManagedResourceStatus.READY);
        ConnectorEntity connector = Fixtures.createConnector(processor,
                "connectorToBeDeleted",
                ManagedResourceStatus.READY,
                "topicName");
        processorDAO.persist(processor);
        connectorsDAO.persist(connector);

        doThrow(new InternalPlatformException(createFailureErrorMessageFor("errorTopic"), new RuntimeException("error")))
                .when(rhoasService).deleteTopicAndRevokeAccessFor(anyString(), any());

        processorService.deleteProcessor(bridge.getId(), processor.getId(), TestConstants.DEFAULT_CUSTOMER_ID);

        reloadAssertProcessorIsInStatus(processor, DEPROVISION);

        waitForProcessorAndConnectorToFail(connector);

        verify(rhoasService).deleteTopicAndRevokeAccessFor(eq("topicName"), eq(RhoasTopicAccessType.PRODUCER));
        verify(connectorsApiClient, never()).deleteConnector(anyString());

        assertShardAsksForProcessorToBeDeletedDoesNotInclude(processor);
    }

    @Test
    public void testDeleteConnectorFailingConnector() {
        Bridge bridge = createPersistBridge(ManagedResourceStatus.READY);
        Processor processor = Fixtures.createProcessor(bridge, "bridgeTestDelete", ManagedResourceStatus.READY);
        ConnectorEntity connector = Fixtures.createConnector(processor,
                "connectorToBeDeleted",
                ManagedResourceStatus.READY,
                "topicName");
        processorDAO.persist(processor);
        connectorsDAO.persist(connector);

        doThrow(new InternalPlatformException(createFailureErrorMessageFor("errorDeletingConnector"), new RuntimeException("error")))
                .when(connectorsApiClient).deleteConnector(anyString());

        processorService.deleteProcessor(bridge.getId(), processor.getId(), TestConstants.DEFAULT_CUSTOMER_ID);

        reloadAssertProcessorIsInStatus(processor, DEPROVISION);

        waitForProcessorAndConnectorToFail(connector);

        verify(rhoasService).deleteTopicAndRevokeAccessFor(eq("topicName"), eq(RhoasTopicAccessType.PRODUCER));
        verify(connectorsApiClient).deleteConnector(anyString());

        assertShardAsksForProcessorToBeDeletedDoesNotInclude(processor);
    }

    private BaseAction createSlackAction() {
        BaseAction mcAction = new BaseAction();
        mcAction.setType(SlackAction.TYPE);
        Map<String, String> parameters = mcAction.getParameters();
        parameters.put("channel", "channel");
        parameters.put("webhookUrl", "webhook_url");
        return mcAction;
    }

    private Connector stubbedExternalConnector(String connectorExternalId) {
        Connector connector = new Connector();
        connector.setId(connectorExternalId);
        return connector;
    }

    private void assertShardAsksForProcessorToBeDeletedIncludes(Processor processor) {
        List<Processor> processorsToBeDeleted = processorDAO.findByStatusesAndShardIdWithReadyDependencies(SHARD_REQUESTED_STATUS, TestConstants.SHARD_ID);
        assertThat(processorsToBeDeleted.stream().map(Processor::getId)).contains(processor.getId());
    }

    private void assertShardAsksForProcessorToBeDeletedDoesNotInclude(Processor processor) {
        List<Processor> processorsToBeDeleted = processorDAO.findByStatusesAndShardIdWithReadyDependencies(SHARD_REQUESTED_STATUS, TestConstants.SHARD_ID);
        assertThat(processorsToBeDeleted.stream().map(Processor::getId)).doesNotContain(processor.getId());
    }

    private void waitForProcessorAndConnectorToFail(ConnectorEntity connector) {
        await().atMost(5, SECONDS).untilAsserted(() -> {
            ConnectorEntity foundConnector = connectorsDAO.findByProcessorIdAndName(connector.getProcessor().getId(), connector.getName());
            assertThat(foundConnector.getStatus()).isEqualTo(ManagedResourceStatus.FAILED);

            reloadAssertProcessorIsInStatus(connector.getProcessor(), ManagedResourceStatus.FAILED);
        });
    }

    private void waitForConnectorToBeDeleted(ConnectorEntity connector) {
        await().atMost(5, SECONDS).untilAsserted(() -> {
            ConnectorEntity foundConnector = connectorsDAO.findByProcessorIdAndName(connector.getProcessor().getId(), "connectorToBeDeleted");
            assertThat(foundConnector).isNull();
        });
    }

    private void reloadAssertProcessorIsInStatus(Processor processor, ManagedResourceStatus status) {
        Processor foundProcessor = processorDAO.findById(processor.getId());
        assertThat(foundProcessor.getStatus()).isEqualTo(status);
    }
}
