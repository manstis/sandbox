package com.redhat.service.smartevents.shard.operator.simplified;

import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Version;

@Group("com.redhat.service.bridge")
@Version("v1alpha1")
@ShortNames("mycr")
public class MyCR extends CustomResource<MyCRSpec, MyCRStatus> implements Namespaced {

}
