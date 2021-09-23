/*
 * Copyright 2010 Proofpoint, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.discovery.client;

import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

///{'environment': 'production_239',
///        'services': [{'id': 'e8c29d83-7bc2-4037-8e6b-4e8b99eb3f7f',
///        'location': '/ffffffff-ffff-ffff-ffff-ffffffffffff',
///        'nodeId': 'ffffffff-ffff-ffff-ffff-ffffffffffff',
///        'pool': 'general',
///        'properties': {'connectorIds': '',
///        'coordinator': 'true',
///        'http': 'http://100.97.80.95:8082',
///        'http-external': 'http://100.97.80.95:8082',
///        'node_version': '0.239-339bacf',
///        'thriftServerPort': '47966'},
///        'type': 'presto'},
///        {'id': '5a3b5027-db65-47ea-beeb-68fce0b6c7f6',
///        'location': '/ffffffff-ffff-ffff-ffff-fffffffffffe',
///        'nodeId': 'ffffffff-ffff-ffff-ffff-fffffffffffe',
///        'pool': 'general',
///        'properties': {'connectorIds': 'de3_mysql,system,de2_clickhouse,de1_es,de3_es,tpch,de1_mysql,tdw_hive,de1_postgre,redis',
///        'coordinator': 'false',
///        'http': 'http://10.101.41.3:8082',
///        'http-external': 'http://10.101.41.3:8082',
///        'node_version': '0.239-339bacf',
///        'thriftServerPort': '33757'},
///        'type': 'presto'}]}

public class ServiceDescriptors
{
    private final String type;
    private final String pool;
    private final String eTag;
    private final Duration maxAge;
    private final List<ServiceDescriptor> serviceDescriptors;

    public ServiceDescriptors(ServiceDescriptors serviceDescriptors,
            Duration maxAge,
            String eTag)
    {
        requireNonNull(serviceDescriptors, "serviceDescriptors is null");

        this.type = serviceDescriptors.type;
        this.pool = serviceDescriptors.pool;
        this.maxAge = maxAge;
        this.eTag = eTag;
        this.serviceDescriptors = serviceDescriptors.serviceDescriptors;
    }

    public ServiceDescriptors(String type,
            String pool,
            List<ServiceDescriptor> serviceDescriptors,
            Duration maxAge,
            String eTag)
    {
        requireNonNull(type, "type is null");
        requireNonNull(serviceDescriptors, "serviceDescriptors is null");
        requireNonNull(maxAge, "maxAge is null");

        this.type = type;
        this.pool = pool;
        this.serviceDescriptors = ImmutableList.copyOf(serviceDescriptors);
        this.maxAge = maxAge;
        this.eTag = eTag;

        // verify service descriptors match expected type
        for (ServiceDescriptor serviceDescriptor : this.serviceDescriptors) {
            if (!type.equals(serviceDescriptor.getType()) || (pool != null && !pool.equals(serviceDescriptor.getPool()))) {
                throw new DiscoveryException(format("Expected %s service descriptor from pool %s, but was %s service descriptor from pool %s",
                        type,
                        pool,
                        serviceDescriptor.getType(),
                        serviceDescriptor.getPool()));
            }
        }
    }

    public String getType()
    {
        return type;
    }

    public String getPool()
    {
        return pool;
    }

    public String getETag()
    {
        return eTag;
    }

    public Duration getMaxAge()
    {
        return maxAge;
    }

    public List<ServiceDescriptor> getServiceDescriptors()
    {
        return serviceDescriptors;
    }
}
