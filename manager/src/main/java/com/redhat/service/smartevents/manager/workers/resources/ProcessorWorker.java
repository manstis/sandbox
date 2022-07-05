package com.redhat.service.smartevents.manager.workers.resources;

import java.util.Objects;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redhat.service.smartevents.infra.models.dto.ManagedResourceStatus;
import com.redhat.service.smartevents.manager.dao.ConnectorsDAO;
import com.redhat.service.smartevents.manager.dao.ProcessorDAO;
import com.redhat.service.smartevents.manager.models.ConnectorEntity;
import com.redhat.service.smartevents.manager.models.Processor;

import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;

import static com.redhat.service.smartevents.manager.workers.WorkManager.STATE_FIELD_ID;

@ApplicationScoped
public class ProcessorWorker extends AbstractWorker<Processor> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessorWorker.class);

    @Inject
    ProcessorDAO processorDAO;

    @Inject
    ConnectorsDAO connectorsDAO;

    @Inject
    ConnectorWorker connectorWorker;

    @Override
    protected PanacheRepositoryBase<Processor, String> getDao() {
        return processorDAO;
    }

    @Override
    protected String getId(JobExecutionContext context) {
        // The ID of the ManagedResource to process is stored directly in the JobDetail.
        JobDataMap data = context.getTrigger().getJobDataMap();
        return data.getString(STATE_FIELD_ID);
    }

    @Override
    public Processor createDependencies(JobExecutionContext context, Processor processor) {
        LOGGER.info("Creating dependencies for '{}' [{}]",
                processor.getName(),
                processor.getId());
        // Transition resource to PREPARING status.
        // PROVISIONING is handled by the Operator.
        processor.setStatus(ManagedResourceStatus.PREPARING);
        processor = persist(processor);

        if (hasZeroConnectors(processor)) {
            LOGGER.debug(
                    "No dependencies required for '{}' [{}]",
                    processor.getName(),
                    processor.getId());
            processor.setDependencyStatus(ManagedResourceStatus.READY);
            return persist(processor);
        }

        // If we have to deploy a Managed Connector, delegate to the ConnectorWorker.
        // The Processor will be provisioned by the Shard when it is in ACCEPTED state *and* Connectors are READY (or null).
        JobDataMap data = context.getTrigger().getJobDataMap();
        String processorId = data.getString(STATE_FIELD_ID);
        ConnectorEntity connectorEntity = connectorsDAO.findByProcessorId(processorId);
        ConnectorEntity updatedConnectorEntity = connectorWorker.createDependencies(context, connectorEntity);
        processor.setDependencyStatus(updatedConnectorEntity.getStatus());

        // If the Connector failed we should mark the Processor as failed too
        if (updatedConnectorEntity.getStatus() == ManagedResourceStatus.FAILED) {
            processor.setStatus(ManagedResourceStatus.FAILED);
        }

        return persist(processor);
    }

    @Override
    protected boolean isProvisioningComplete(Processor managedResource) {
        //As far as the Worker mechanism is concerned work for a Processor is complete when the dependencies are complete.
        return PROVISIONING_COMPLETED.contains(managedResource.getDependencyStatus());
    }

    @Override
    public Processor deleteDependencies(JobExecutionContext context, Processor processor) {
        LOGGER.info("Destroying dependencies for '{}' [{}]",
                processor.getName(),
                processor.getId());

        if (hasZeroConnectors(processor)) {
            LOGGER.debug("No dependencies required for '{}' [{}]",
                    processor.getName(),
                    processor.getId());
            processor.setDependencyStatus(ManagedResourceStatus.DELETED);
            return persist(processor);
        }

        // If we have to delete a Managed Connector, delegate to the ConnectorWorker.
        JobDataMap data = context.getTrigger().getJobDataMap();
        String processorId = data.getString(STATE_FIELD_ID);
        ConnectorEntity connectorEntity = connectorsDAO.findByProcessorId(processorId);
        ConnectorEntity updatedConnectorEntity = connectorWorker.deleteDependencies(context, connectorEntity);
        processor.setDependencyStatus(updatedConnectorEntity.getStatus());

        // If the Connector failed we should mark the Processor as failed too
        if (updatedConnectorEntity.getStatus() == ManagedResourceStatus.FAILED) {
            processor.setStatus(ManagedResourceStatus.FAILED);
        }

        return persist(processor);
    }

    @Override
    protected boolean isDeprovisioningComplete(Processor managedResource) {
        //As far as the Worker mechanism is concerned work for a Processor is complete when the dependencies are complete.
        return DEPROVISIONING_COMPLETED.contains(managedResource.getDependencyStatus());
    }

    protected boolean hasZeroConnectors(Processor processor) {
        return Objects.isNull(getConnectorEntity(processor));
    }

    protected ConnectorEntity getConnectorEntity(Processor processor) {
        return connectorsDAO.findByProcessorId(processor.getId());
    }
}
