/**
 * 当前 ElasticsearchSinkBuilder 设计参考：https://github.com/mtfelisb/flink-connector-elasticsearch
 *
 */

/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package com.monchickey.sink;

import org.apache.http.HttpHost;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class ElasticsearchSinkBuilder {

    private HttpHost httpHost;

    private String username;

    private String password;

    private Integer batchSize;
    private Long timeInterval;



    public ElasticsearchSinkBuilder setHttpHost(HttpHost httpHost) {
        checkNotNull(httpHost);
        this.httpHost = httpHost;
        return this;
    }

    public ElasticsearchSinkBuilder setUsername(String username) {
        checkNotNull(username);
        this.username = username;
        return this;
    }


    public ElasticsearchSinkBuilder setPassword(String password) {
        checkNotNull(password);
        this.password = password;
        return this;
    }


    public ElasticsearchSinkBuilder setBatchSize(Integer batchSize) {
        checkNotNull(batchSize);
        checkState(batchSize > 0, "Batch size should be positive");
        this.batchSize = batchSize;
        return this;
    }

    public ElasticsearchSinkBuilder setTimeInterval(Long timeInterval) {
        checkNotNull(timeInterval);
        checkState(timeInterval > 0, "Time interval should be positive");
        this.timeInterval = timeInterval;
        return this;
    }

    /**
     * build
     * the Elasticsearch sink
     *
     * @return the {ElasticsearchSink} instance
     */
    public ElasticsearchSink build() {
        validate();

        return new ElasticsearchSink (
            new ElasticsearchConfigFactory(httpHost, username, password),
            batchSize,
            timeInterval
        );
    }

    public static ElasticsearchSinkBuilder builder() {
        return new ElasticsearchSinkBuilder();
    }

    private void validate() {
        this.setHttpHost(this.httpHost);
        this.setBatchSize(this.batchSize);
        this.setTimeInterval(this.timeInterval);
    }
}
