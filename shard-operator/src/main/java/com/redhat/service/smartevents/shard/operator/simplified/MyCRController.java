package com.redhat.service.smartevents.shard.operator.simplified;

import javax.enterprise.context.ApplicationScoped;

import com.redhat.service.smartevents.shard.operator.utils.LabelsBuilder;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

@ApplicationScoped
@ControllerConfiguration(labelSelector = LabelsBuilder.RECONCILER_LABEL_SELECTOR)
public class MyCRController implements Reconciler<MyCR> {

    @Override
    public UpdateControl<MyCR> reconcile(MyCR myCR, Context context) {
        System.out.println("Reconciling....");
        return UpdateControl.noUpdate();
    }

    @Override
    public DeleteControl cleanup(MyCR myCR, Context context) {
        System.out.println("Deleting....");
        return DeleteControl.defaultDelete();
    }

}
