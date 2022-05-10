package com.redhat.service.smartevents.infra.models;

import javax.ws.rs.QueryParam;

import com.redhat.service.smartevents.infra.models.dto.ManagedResourceStatus;

import static com.redhat.service.smartevents.infra.api.APIConstants.FILTER_NAME;
import static com.redhat.service.smartevents.infra.api.APIConstants.FILTER_STATUS;

public class QueryFilterInfo {

    @QueryParam(FILTER_NAME)
    private String filterName;

    @QueryParam(FILTER_STATUS)
    private ManagedResourceStatus filterStatus;

    public QueryFilterInfo() {
        this(null, null);
    }

    public QueryFilterInfo(String filterName) {
        this(filterName, null);
    }

    public QueryFilterInfo(ManagedResourceStatus filterStatus) {
        this(null, filterStatus);
    }

    public QueryFilterInfo(String filterName, ManagedResourceStatus filterStatus) {
        this.filterName = filterName;
        this.filterStatus = filterStatus;
    }

    public String getFilterName() {
        return filterName;
    }

    public ManagedResourceStatus getFilterStatus() {
        return filterStatus;
    }

    @Override
    public String toString() {
        return "QueryFilterInfo{" +
                "filterName='" + filterName + '\'' +
                ", filterStatus=" + filterStatus +
                '}';
    }
}
