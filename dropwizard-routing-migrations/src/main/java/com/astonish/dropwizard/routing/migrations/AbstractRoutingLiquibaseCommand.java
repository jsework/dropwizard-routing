/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.astonish.dropwizard.routing.migrations;

import com.astonish.dropwizard.routing.db.DataSourceRoute;
import com.astonish.dropwizard.routing.db.RoutingDatabaseConfiguration;
import com.codahale.dropwizard.Configuration;
import com.codahale.dropwizard.cli.ConfiguredCommand;
import com.codahale.dropwizard.db.DataSourceFactory;
import com.codahale.dropwizard.db.ManagedDataSource;
import com.codahale.dropwizard.migrations.CloseableLiquibase;
import com.codahale.dropwizard.setup.Bootstrap;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;

import liquibase.Liquibase;
import liquibase.exception.LiquibaseException;
import liquibase.exception.ValidationFailedException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.sql.SQLException;

/**
 * Provides routing support for liquibase migrations.
 */
public abstract class AbstractRoutingLiquibaseCommand<T extends Configuration> extends ConfiguredCommand<T> {
    private final RoutingDatabaseConfiguration<T> strategy;
    private final Class<T> configurationClass;

    protected AbstractRoutingLiquibaseCommand(String name, String description,
            RoutingDatabaseConfiguration<T> strategy, Class<T> configurationClass) {
        super(name, description);
        this.strategy = strategy;
        this.configurationClass = configurationClass;
    }

    @Override
    protected Class<T> getConfigurationClass() {
        return configurationClass;
    }

    @Override
    public void configure(Subparser subparser) {
        super.configure(subparser);

        subparser.addArgument("--migrations").dest("migrations-file")
                .help("the file containing the Liquibase migrations for the application");

        // only issue liquibase command to one route
        subparser.addArgument("--route").dest("routeName").help("the database route name");
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.codahale.dropwizard.cli.ConfiguredCommand#run(com.codahale.dropwizard.setup.Bootstrap,
     * net.sourceforge.argparse4j.inf.Namespace, com.codahale.dropwizard.Configuration)
     */
    @Override
    protected void run(Bootstrap<T> bootstrap, Namespace namespace, T configuration) throws Exception {
        final String routeName = (String) namespace.get("routeName");

        // if route is specified then only run the liquibase command against that route
        if (null != routeName) {
            final ImmutableList<DataSourceRoute> routes = strategy.getDataSourceRoutes(configuration);

            for (DataSourceRoute route : routes) {
                if (routeName.equals(route.getRouteName())) {
                    run(route, namespace, configuration);
                }
            }
        } else {
            // run against all routes
            for (DataSourceRoute route : strategy.getDataSourceRoutes(configuration)) {
                run(route, namespace, configuration);
            }
        }
    }

    private void run(DataSourceRoute route, Namespace namespace, T configuration) throws Exception {
        final DataSourceFactory dbConfig = route.getDatabase();
        dbConfig.setMaxSize(1);
        dbConfig.setMinSize(1);
        dbConfig.setInitialSize(1);

        try (CloseableLiquibase liquibase = openLiquibase(dbConfig, namespace)) {
            run(namespace, liquibase);
        } catch (ValidationFailedException e) {
            e.printDescriptiveError(System.err);
            throw e;
        }
    }

    private CloseableLiquibase openLiquibase(DataSourceFactory dataSourceFactory, Namespace namespace)
            throws ClassNotFoundException, SQLException, LiquibaseException {
        final ManagedDataSource dataSource = dataSourceFactory.build(new MetricRegistry(), "liquibase");
        final String migrationsFile = (String) namespace.get("migrations-file");
        if (migrationsFile == null) {
            return new CloseableLiquibase(dataSource);
        }
        return new CloseableLiquibase(dataSource, migrationsFile);
    }

    protected abstract void run(Namespace namespace, Liquibase liquibase) throws Exception;
}