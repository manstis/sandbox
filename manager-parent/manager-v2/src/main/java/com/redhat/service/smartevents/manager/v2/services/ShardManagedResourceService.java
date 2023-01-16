package com.redhat.service.smartevents.manager.v2.services;

import java.util.List;

import com.redhat.service.smartevents.infra.v2.api.models.dto.BaseV2DTO;
import com.redhat.service.smartevents.infra.v2.api.models.dto.ResourceStatusDTO;
import com.redhat.service.smartevents.manager.v2.persistence.models.ManagedResourceV2;

public interface ShardManagedResourceService<R extends ManagedResourceV2, D extends BaseV2DTO> {
    List<R> findByShardIdToDeployOrDelete(String shardId);

    R updateStatus(ResourceStatusDTO statusDTO);

    D toDTO(R resource);
}
