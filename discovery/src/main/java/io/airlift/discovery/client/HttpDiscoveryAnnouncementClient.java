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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.CharStreams;
import com.google.common.net.HttpHeaders;
import com.google.common.net.MediaType;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.CacheControl;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.json.JsonCodec;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.preparePut;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;


///{'environment': 'production_239',
/// 'location': '/ffffffff-ffff-ffff-ffff-ffffffffffff',
/// 'nodeId': 'ffffffff-ffff-ffff-ffff-ffffffffffff',
/// 'pool': 'general',
/// 'services': [{'id': '4172ed48-2828-4e31-8677-fc5770e6ba8e',
///               'properties': {'http': 'http://100.97.80.95:8082',
///                              'http-external': 'http://100.97.80.95:8082'},
///               'type': 'presto-coordinator'}, /// coordinator discovery-server
///              {'id': 'b2016a5c-0ad9-4f68-af21-3f5558af8539',
///               'properties': {'http': 'http://100.97.80.95:8082',
///                              'http-external': 'http://100.97.80.95:8082'},
///               'type': 'discovery'}, /// coordinator discovery-server
///              {'id': '35b61a31-443b-43fb-9224-bd8441bed4f6',
///               'properties': {'connectorIds': 'de3_mysql,system,de2_clickhouse,de1_es,tpch,de3_es,de1_mysql,tdw_hive,de1_postgre,redis',
///                              'coordinator': 'true',
///                              'http': 'http://100.97.80.95:8082',
///                              'http-external': 'http://100.97.80.95:8082',
///                              'node_version': '0.239-339bacf',
///                              'thriftServerPort': '44242'},
///               'type': 'presto'}, /// presto node
///              {'id': '84aab31d-c4ef-44be-871e-03208e6f070e',
///               'properties': {'http': 'http://100.97.80.95:8082',
///                              'http-external': 'http://100.97.80.95:8082'},
///               'type': 'jmx-http'}, ///airlift
///              {'id': '62c88839-5ebe-47c5-9aed-3cb326e0a06f',
///               'properties': {'jmx': 'service:jmx:rmi:///jndi/rmi://Tencent-SNG:31763/jmxrmi'},
///               'type': 'jmx'}]} /// JmxModule


/// {'environment': 'production_239',
///  'location': '/ffffffff-ffff-ffff-ffff-fffffffffffe',
///  'nodeId': 'ffffffff-ffff-ffff-ffff-fffffffffffe',
///  'pool': 'general',
///  'services': [{'id': '5a3b5027-db65-47ea-beeb-68fce0b6c7f6',
///                'properties': {'connectorIds': 'de3_mysql,system,de2_clickhouse,de1_es,de3_es,tpch,de1_mysql,tdw_hive,de1_postgre,redis',
///                               'coordinator': 'false',
///                               'http': 'http://10.101.41.3:8082',
///                               'http-external': 'http://10.101.41.3:8082',
///                               'node_version': '0.239-339bacf',
///                               'thriftServerPort': '33757'},
///                'type': 'presto'},
///               {'id': '5d7cdbdf-79ac-4b4e-b574-6c1216454513',
///                'properties': {'jmx': 'service:jmx:rmi:///jndi/rmi://SNG-Qcloud:38626/jmxrmi'},
///                'type': 'jmx'},
///               {'id': 'd643671a-cf4a-42f3-9f36-98dd89bf562b',
///                'properties': {'http': 'http://10.101.41.3:8082',
///                               'http-external': 'http://10.101.41.3:8082'},
///                'type': 'jmx-http'}]}
public class HttpDiscoveryAnnouncementClient
        implements DiscoveryAnnouncementClient
{
    private static final MediaType MEDIA_TYPE_JSON = MediaType.create("application", "json");

    private final Supplier<URI> discoveryServiceURI;
    private final NodeInfo nodeInfo;
    private final JsonCodec<Announcement> announcementCodec;
    private final HttpClient httpClient;

    @Inject
    public HttpDiscoveryAnnouncementClient(
            @ForDiscoveryClient Supplier<URI> discoveryServiceURI,
            NodeInfo nodeInfo,
            JsonCodec<Announcement> announcementCodec,
            @ForDiscoveryClient HttpClient httpClient)
    {
        requireNonNull(discoveryServiceURI, "discoveryServiceURI is null");
        requireNonNull(nodeInfo, "nodeInfo is null");
        requireNonNull(announcementCodec, "announcementCodec is null");
        requireNonNull(httpClient, "httpClient is null");

        this.nodeInfo = nodeInfo;
        this.discoveryServiceURI = discoveryServiceURI;
        this.announcementCodec = announcementCodec;
        this.httpClient = httpClient;
    }

    @Override
    public ListenableFuture<Duration> announce(Set<ServiceAnnouncement> services) ///向discovery service发布自己的服务。
    {
        requireNonNull(services, "services is null");

        URI uri = discoveryServiceURI.get();
        if (uri == null) {
            return immediateFailedFuture(new DiscoveryException("No discovery servers are available"));
        }

        Announcement announcement = new Announcement(nodeInfo.getEnvironment(), nodeInfo.getNodeId(), nodeInfo.getPool(), nodeInfo.getLocation(), services);
        Request request = preparePut()
                .setUri(createAnnouncementLocation(uri, nodeInfo.getNodeId()))
                .setHeader("User-Agent", nodeInfo.getNodeId())
                .setHeader("Content-Type", MEDIA_TYPE_JSON.toString())
                .setBodyGenerator(jsonBodyGenerator(announcementCodec, announcement)) /// 发布自己拥有的服务。
                .build();
        return httpClient.executeAsync(request, new DiscoveryResponseHandler<Duration>("Announcement", uri)
        {
            @Override
            public Duration handle(Request request, Response response)
                    throws DiscoveryException
            {
                int statusCode = response.getStatusCode();
                if (!isSuccess(statusCode)) {
                    throw new DiscoveryException(String.format("Announcement failed with status code %s: %s", statusCode, getBodyForError(response)));
                }

                Duration maxAge = extractMaxAge(response);
                return maxAge;
            }
        });
    }

    private static boolean isSuccess(int statusCode)
    {
        return statusCode / 100 == 2;
    }

    private static String getBodyForError(Response response)
    {
        try {
            return CharStreams.toString(new InputStreamReader(response.getInputStream(), UTF_8));
        }
        catch (IOException e) {
            return "(error getting body)";
        }
    }

    @Override
    public ListenableFuture<Void> unannounce()
    {
        URI uri = discoveryServiceURI.get();
        if (uri == null) {
            return immediateFuture(null);
        }

        Request request = prepareDelete()
                .setUri(createAnnouncementLocation(uri, nodeInfo.getNodeId()))
                .setHeader("User-Agent", nodeInfo.getNodeId())
                .build();
        return httpClient.executeAsync(request, new DiscoveryResponseHandler<>("Unannouncement", uri));
    }

    @VisibleForTesting
    static URI createAnnouncementLocation(URI baseUri, String nodeId)
    {
        return uriBuilderFrom(baseUri)
                .appendPath("/v1/announcement")
                .appendPath(nodeId)
                .build();
    }

    private static Duration extractMaxAge(Response response)
    {
        String header = response.getHeader(HttpHeaders.CACHE_CONTROL);
        if (header != null) {
            CacheControl cacheControl = CacheControl.valueOf(header);
            if (cacheControl.getMaxAge() > 0) {
                return new Duration(cacheControl.getMaxAge(), TimeUnit.SECONDS);
            }
        }
        return DEFAULT_DELAY;
    }

    private class DiscoveryResponseHandler<T>
            implements ResponseHandler<T, DiscoveryException>
    {
        private final String name;
        private final URI uri;

        protected DiscoveryResponseHandler(String name, URI uri)
        {
            this.name = name;
            this.uri = uri;
        }

        @Override
        public T handle(Request request, Response response)
        {
            return null;
        }

        @Override
        public final T handleException(Request request, Exception exception)
        {
            if (exception instanceof InterruptedException) {
                throw new DiscoveryException(name + " was interrupted for " + uri);
            }
            if (exception instanceof CancellationException) {
                throw new DiscoveryException(name + " was canceled for " + uri);
            }
            if (exception instanceof DiscoveryException) {
                throw (DiscoveryException) exception;
            }

            throw new DiscoveryException(name + " failed for " + uri, exception);
        }
    }
}
