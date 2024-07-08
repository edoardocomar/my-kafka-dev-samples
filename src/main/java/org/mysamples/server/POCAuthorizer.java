/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.mysamples.server;

import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletionStage;

public class POCAuthorizer implements Authorizer {

    public static final Logger logger = LoggerFactory.getLogger("or.mysamples.server.POCAuthorizer");
    private boolean isController;

    @Override
    public void configure(Map<String, ?> configs) {
        isController = "controller".equals(configs.get("process.roles"));
        logger.warn("POCAuthorizer configured : process.roles=" + configs.get("process.roles") + " isController=" + isController);
    }

    @Override
    public Map<Endpoint, ? extends CompletionStage<Void>> start(AuthorizerServerInfo serverInfo) {
        return Collections.emptyMap();
    }

    @Override
    public List<AuthorizationResult> authorize(AuthorizableRequestContext requestContext, List<Action> actions) {
        List<AuthorizationResult> results = new ArrayList<>(actions.size());
        for (int i = 0; i < actions.size(); i++) {
            Action action = actions.get(i);
            ResourceType resourceType = action.resourcePattern().resourceType();
            if (requestContext.requestType() == ApiKeys.CREATE_TOPICS.id) {
                logger.warn("POCAuthorizer requestContext.requestType == ApiKeys.CREATE_TOPICS");
                if (action.operation() == AclOperation.CREATE && resourceType == ResourceType.CLUSTER) {
                    logger.warn("POCAuthorizer DENY action " + action + " //CREATE on CLUSTER");
                    results.add(AuthorizationResult.DENIED);
                    continue;
                } else if (resourceType == ResourceType.TOPIC && action.resourcePattern().name().toUpperCase(Locale.ENGLISH).startsWith("Z")) {
                    logger.warn("POCAuthorizer DENY action " + action + " //topic startsWith Z");
                    results.add(AuthorizationResult.DENIED);
                    continue;
//                    throw new ESAuthException("Topic starts with Z");
                }
            }
            results.add(AuthorizationResult.ALLOWED);
        }
        return results;
    }

    @Override
    public List<? extends CompletionStage<AclCreateResult>> createAcls(AuthorizableRequestContext requestContext, List<AclBinding> aclBindings) {
        return Collections.emptyList();
    }

    @Override
    public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(AuthorizableRequestContext requestContext, List<AclBindingFilter> aclBindingFilters) {
        return Collections.emptyList();
    }

    @Override
    public Iterable<AclBinding> acls(AclBindingFilter filter) {
        return Collections.emptyList();
    }

    @Override
    public void close() throws IOException {
    }
}
