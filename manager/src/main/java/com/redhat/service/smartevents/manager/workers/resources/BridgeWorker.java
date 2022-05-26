package com.redhat.service.smartevents.manager.workers.resources;

import java.util.Objects;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.service.smartevents.infra.models.ListResult;
import com.redhat.service.smartevents.infra.models.dto.ManagedResourceStatus;
import com.redhat.service.smartevents.infra.models.gateways.Action;
import com.redhat.service.smartevents.manager.ProcessorService;
import com.redhat.service.smartevents.manager.RhoasService;
import com.redhat.service.smartevents.manager.api.models.requests.ProcessorRequest;
import com.redhat.service.smartevents.manager.dao.BridgeDAO;
import com.redhat.service.smartevents.manager.models.Bridge;
import com.redhat.service.smartevents.manager.models.Processor;
import com.redhat.service.smartevents.manager.models.Work;
import com.redhat.service.smartevents.manager.providers.ResourceNamesProvider;
import com.redhat.service.smartevents.rhoas.RhoasTopicAccessType;

import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import io.quarkus.vertx.ConsumeEvent;

@ApplicationScoped
public class BridgeWorker extends AbstractWorker<Bridge> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BridgeWorker.class);

    @Inject
    BridgeDAO bridgeDAO;

    @Inject
    RhoasService rhoasService;

    @Inject
    ResourceNamesProvider resourceNamesProvider;

    @Inject
    ProcessorService processorService;

    @Override
    protected PanacheRepositoryBase<Bridge, String> getDao() {
        return bridgeDAO;
    }

    // This must be equal to the Bridge.class.getName()
    @ConsumeEvent(value = "com.redhat.service.smartevents.manager.models.Bridge", blocking = true)
    public Bridge handleWork(Work work) {
        return super.handleWork(work);
    }

    @Override
    public Bridge createDependencies(Work work, Bridge bridge) {
        LOGGER.info("Creating dependencies for '{}' [{}]",
                bridge.getName(),
                bridge.getId());
        // Transition resource to PREPARING status.
        // PROVISIONING is handled by the Operator.
        bridge.setStatus(ManagedResourceStatus.PREPARING);

        // This is idempotent as it gets overridden later depending on actual state
        bridge.setDependencyStatus(ManagedResourceStatus.PROVISIONING);
        bridge = persist(bridge);

        // If this call throws an exception the Bridge's dependencies will be left in PROVISIONING state...
        rhoasService.createTopicAndGrantAccessFor(resourceNamesProvider.getBridgeTopicName(bridge.getId()),
                RhoasTopicAccessType.CONSUMER_AND_PRODUCER);

        // Create back-channel topic
        rhoasService.createTopicAndGrantAccessFor(resourceNamesProvider.getErrorHandlerTopicName(bridge.getId()),
                RhoasTopicAccessType.CONSUMER_AND_PRODUCER);

        // We don't need to wait for the Bridge to be READY to create the Error Handler.
        // If this **IS** a problem we can move this to the "BridgeServiceImpl#updateBridge(...)" method and
        // create the Processor when the Bridge status changes to READY. Having creation here is consistent
        // with our Worker mechanism.
        if (isErrorHandlerProcessorReady(bridge)) {
            bridge.setDependencyStatus(ManagedResourceStatus.READY);
        }

        return persist(bridge);
    }

    private boolean isErrorHandlerProcessorReady(Bridge bridge) {
        // If an ErrorHandler is not needed, consider it ready
        Action errorHandlerAction = bridge.getDefinition().getErrorHandler();
        if (Objects.isNull(errorHandlerAction)) {
            return true;
        }

        String bridgeId = bridge.getId();
        String customerId = bridge.getCustomerId();
        ListResult<Processor> processors = processorService.getAllProcessors(bridgeId, customerId);
        // This assumes we can only have one ErrorHandler Processor per Bridge
        if (processors.getItems().stream().noneMatch(p -> p.getDefinition().isErrorHandler())) {
            String errorHandlerName = String.format("Back-channel for Bridge '%s'", bridge.getId());
            ProcessorRequest errorHandlerProcessor = new ProcessorRequest(errorHandlerName, errorHandlerAction);
            processorService.createErrorHandlingProcessor(bridge.getId(), bridge.getCustomerId(), bridge.getOwner(), errorHandlerProcessor);
            return false;
        }
        return true;
    }

    @Override
    protected boolean isProvisioningComplete(Bridge managedResource) {
        //As far as the Worker mechanism is concerned work for a Bridge is complete when the dependencies are complete.
        return PROVISIONING_COMPLETED.contains(managedResource.getDependencyStatus());
    }

    @Override
    public Bridge deleteDependencies(Work work, Bridge bridge) {
        LOGGER.info("Destroying dependencies for '{}' [{}]",
                bridge.getName(),
                bridge.getId());
        // This is idempotent as it gets overridden later depending on actual state
        bridge.setDependencyStatus(ManagedResourceStatus.DELETING);
        bridge = persist(bridge);

        // If this call throws an exception the Bridge's dependencies will be left in DELETING state...
        rhoasService.deleteTopicAndRevokeAccessFor(resourceNamesProvider.getBridgeTopicName(bridge.getId()),
                RhoasTopicAccessType.CONSUMER_AND_PRODUCER);

        // Delete back-channel topic
        rhoasService.deleteTopicAndRevokeAccessFor(resourceNamesProvider.getErrorHandlerTopicName(bridge.getId()),
                RhoasTopicAccessType.CONSUMER_AND_PRODUCER);

        return persist(bridge);
    }

    @Override
    protected boolean isDeprovisioningComplete(Bridge managedResource) {
        //As far as the Worker mechanism is concerned work for a Bridge is complete when the dependencies are complete.
        return DEPROVISIONING_COMPLETED.contains(managedResource.getDependencyStatus());
    }
}
