package com.redhat.service.smartevents.shard.operator.simplified;

import java.util.HashSet;

import com.redhat.service.smartevents.shard.operator.resources.Condition;
import com.redhat.service.smartevents.shard.operator.resources.ConditionStatus;
import com.redhat.service.smartevents.shard.operator.resources.ConditionTypeConstants;
import com.redhat.service.smartevents.shard.operator.resources.CustomResourceStatus;

public class MyCRStatus extends CustomResourceStatus {

    private static final HashSet<Condition> CONDITIONS = new HashSet<>() {
        {
            add(new Condition(ConditionTypeConstants.READY, ConditionStatus.Unknown));
        }
    };

    public MyCRStatus() {
        super(CONDITIONS);
    }
}
