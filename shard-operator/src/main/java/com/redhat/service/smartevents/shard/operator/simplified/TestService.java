package com.redhat.service.smartevents.shard.operator.simplified;

public interface TestService {
    void create(String name);

    void delete(String name);

    void deleteByNamespace();

}
