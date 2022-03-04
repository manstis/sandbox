package com.redhat.service.bridge.manager.models;

import java.time.ZonedDateTime;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.NamedQueries;
import javax.persistence.NamedQuery;
import javax.persistence.Version;

@NamedQueries({
        @NamedQuery(name = "Work.findByManagedResourceId", query = "from Work w where w.managedResourceId=:managedResourceId"),
        @NamedQuery(name = "Work.findByWorkerId", query = "from Work w where w.workerId=:workerId"),
        @NamedQuery(name = "Work.updateWorkerId", query = "update Work w set w.workerId=:workerId where w.submittedAt >= :age")
})
@Entity
public class Work {

    @Id
    private String id = UUID.randomUUID().toString();

    @Column(name = "managed_resource_id", nullable = false, updatable = false)
    private String managedResourceId;

    @Column(nullable = false, updatable = false)
    private String type;

    @Column(name = "worker_id", nullable = false)
    private String workerId;

    @Column(name = "submitted_at", nullable = false)
    private ZonedDateTime submittedAt;

    @Column(name = "modified_at", columnDefinition = "TIMESTAMP", nullable = false)
    private ZonedDateTime modifiedAt;

    @Column(name = "attempts", nullable = false)
    private int attempts = 0;

    @Version
    @SuppressWarnings("unused")
    private long version;

    public String getId() {
        return id;
    }

    public String getManagedResourceId() {
        return managedResourceId;
    }

    public void setManagedResourceId(String entityId) {
        this.managedResourceId = entityId;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public ZonedDateTime getSubmittedAt() {
        return submittedAt;
    }

    public void setSubmittedAt(ZonedDateTime submittedAt) {
        this.submittedAt = submittedAt;
    }

    public ZonedDateTime getModifiedAt() {
        return modifiedAt;
    }

    public void setModifiedAt(ZonedDateTime modifiedAt) {
        this.modifiedAt = modifiedAt;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public static Work forResource(ManagedResource managedResource, String workerId) {
        Work w = new Work();
        w.setSubmittedAt(ZonedDateTime.now());
        w.setModifiedAt(ZonedDateTime.now());
        w.setType(managedResource.getClass().getSimpleName());
        w.setManagedResourceId(managedResource.getId());
        w.setWorkerId(workerId);
        return w;
    }

    @Override
    public String toString() {
        return "Work{" +
                "id='" + id + '\'' +
                ", managedResourceId='" + managedResourceId + '\'' +
                ", type='" + type + '\'' +
                ", workerId='" + workerId + '\'' +
                ", submittedAt=" + submittedAt +
                ", modifiedAt=" + modifiedAt +
                ", attempts=" + attempts +
                '}';
    }
}