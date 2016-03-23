/*
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
package com.facebook.presto.plugin.crate;

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcConnectorId;
import com.facebook.presto.plugin.jdbc.JdbcMetadata;
import com.facebook.presto.plugin.jdbc.JdbcMetadataConfig;
import com.facebook.presto.plugin.jdbc.JdbcSplitManager;
import com.facebook.presto.spi.type.TypeManager;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class CrateClientModule
        implements Module
{
    private final String      connectorId;
    private final TypeManager typeManager;

    public CrateClientModule(String connectorId, TypeManager typeManager)
    {
        this.connectorId = connectorId;
        this.typeManager = typeManager;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(CrateConnector.class).in(Scopes.SINGLETON);
        binder.bind(TypeManager.class).toInstance(typeManager);
        binder.bind(CrateClient.class).in(Scopes.SINGLETON);
        binder.bind(JdbcClient.class).to(CrateClient.class).in(Scopes.SINGLETON);
        binder.bind(JdbcMetadata.class).in(Scopes.SINGLETON);
        binder.bind(JdbcMetadataConfig.class).in(Scopes.SINGLETON);
        binder.bind(JdbcSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(CrateRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(JdbcConnectorId.class).toInstance(new JdbcConnectorId(connectorId));
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
    }
}
