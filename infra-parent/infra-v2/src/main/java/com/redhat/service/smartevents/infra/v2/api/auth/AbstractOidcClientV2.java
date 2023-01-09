package com.redhat.service.smartevents.infra.v2.api.auth;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

import com.redhat.service.smartevents.infra.core.auth.AbstractOidcClient;
import com.redhat.service.smartevents.infra.core.exceptions.definitions.platform.InternalPlatformException;
import com.redhat.service.smartevents.infra.v2.api.exceptions.definitions.platform.OidcTokensNotInitializedException;

import io.quarkus.oidc.client.OidcClients;

public abstract class AbstractOidcClientV2 extends AbstractOidcClient {

    public AbstractOidcClientV2() {
    }

    public AbstractOidcClientV2(String name, OidcClients oidcClients, Duration timeout, ScheduledExecutorService executorService) {
        super(name, oidcClients, timeout, executorService);
    }

    public AbstractOidcClientV2(String name, OidcClients oidcClients, ScheduledExecutorService executorService) {
        super(name, oidcClients, executorService);
    }

    protected InternalPlatformException getOidcTokensNotInitializedException(String message) {
        return new OidcTokensNotInitializedException(message);
    }
}
