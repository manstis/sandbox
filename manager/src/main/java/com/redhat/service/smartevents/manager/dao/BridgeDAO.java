package com.redhat.service.smartevents.manager.dao;

import java.util.List;
import java.util.Objects;

import javax.enterprise.context.ApplicationScoped;
import javax.transaction.Transactional;

import com.redhat.service.smartevents.infra.models.ListResult;
import com.redhat.service.smartevents.infra.models.QueryResourceInfo;
import com.redhat.service.smartevents.infra.models.dto.ManagedResourceStatus;
import com.redhat.service.smartevents.manager.models.Bridge;

import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import io.quarkus.panache.common.Parameters;

@ApplicationScoped
@Transactional
public class BridgeDAO implements PanacheRepositoryBase<Bridge, String> {

    public List<Bridge> findByShardIdWithReadyDependencies(String shardId) {
        Parameters params = Parameters
                .with("shardId", shardId);
        return find("#BRIDGE.findByShardIdWithReadyDependencies", params).list();
    }

    public Bridge findByNameAndCustomerId(String name, String customerId) {
        Parameters params = Parameters
                .with("name", name).and("customerId", customerId);
        return find("#BRIDGE.findByNameAndCustomerId", params).firstResult();
    }

    public Bridge findByIdAndCustomerId(String id, String customerId) {
        Parameters params = Parameters
                .with("id", id).and("customerId", customerId);
        return find("#BRIDGE.findByIdAndCustomerId", params).firstResult();
    }

    public ListResult<Bridge> findByCustomerId(String customerId, QueryResourceInfo queryInfo) {
        Parameters parameters = Parameters.with("customerId", customerId);
        long total;
        List<Bridge> bridges;
        String filterName = queryInfo.getFilterInfo().getFilterName();
        ManagedResourceStatus filterStatus = queryInfo.getFilterInfo().getFilterStatus();
        if (Objects.isNull(filterName) && Objects.isNull(filterStatus)) {
            total = find("#BRIDGE.findByCustomerId", parameters).count();
            bridges = find("#BRIDGE.findByCustomerId", parameters).page(queryInfo.getPageNumber(), queryInfo.getPageSize()).list();
        } else if (Objects.isNull(filterStatus)) {
            parameters = parameters.and("name", filterName);
            total = find("#BRIDGE.findByCustomerIdFilterByName", parameters).count();
            bridges = find("#BRIDGE.findByCustomerIdFilterByName", parameters).page(queryInfo.getPageNumber(), queryInfo.getPageSize()).list();
        } else if (Objects.isNull(filterName)) {
            parameters = parameters.and("status", filterStatus);
            total = find("#BRIDGE.findByCustomerIdFilterByStatus", parameters).count();
            bridges = find("#BRIDGE.findByCustomerIdFilterByStatus", parameters).page(queryInfo.getPageNumber(), queryInfo.getPageSize()).list();
        } else {
            parameters = parameters.and("name", filterName);
            parameters = parameters.and("status", filterStatus);
            total = find("#BRIDGE.findByCustomerIdFilterByNameAndStatus", parameters).count();
            bridges = find("#BRIDGE.findByCustomerIdFilterByNameAndStatus", parameters).page(queryInfo.getPageNumber(), queryInfo.getPageSize()).list();
        }

        return new ListResult<>(bridges, queryInfo.getPageNumber(), total);
    }
}
