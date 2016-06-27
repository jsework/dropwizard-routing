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
package com.astonish.dropwizard.routing.hibernate;

import io.dropwizard.Configuration;
import io.dropwizard.ConfiguredBundle;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.hibernate.SessionFactoryHealthCheck;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.dropwizard.util.Duration;

import java.util.LinkedHashMap;
import java.util.Map;

import com.astonish.dropwizard.routing.db.DataSourceRoute;
import com.astonish.dropwizard.routing.db.RoutingDatabaseConfiguration;
import com.fasterxml.jackson.datatype.hibernate4.Hibernate4Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.hibernate.SessionFactory;

/**
 * Routing Hibernate bundle.
 */
public abstract class RoutingHibernateBundle<T extends Configuration> implements ConfiguredBundle<T>,
        RoutingDatabaseConfiguration<T> {
    private ImmutableMap<String, SessionFactory> sessionFactoryMap;
    private final ImmutableList<Class<?>> entities;
    private final RoutingSessionFactoryFactory sessionFactoryFactory;

    /**
     * @param entity
     *            the first Hibernate entity
     * @param entities
     *            all other Hibernate entities
     */
    public RoutingHibernateBundle(Class<?> entity, Class<?>... entities) {
        this(ImmutableList.<Class<?>> builder().add(entity).add(entities).build(), new RoutingSessionFactoryFactory());
    }

    /**
     * @param entities
     *            the Hibernate entities
     * @param sessionFactoryFactory
     *            the {@link RoutingSessionFactoryFactory}
     */
    public RoutingHibernateBundle(ImmutableList<Class<?>> entities, RoutingSessionFactoryFactory sessionFactoryFactory) {
        this.entities = entities;
        this.sessionFactoryFactory = sessionFactoryFactory;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.codahale.dropwizard.ConfiguredBundle#initialize(com.codahale.dropwizard.setup.Bootstrap)
     */
    @Override
    public final void initialize(Bootstrap<?> bootstrap) {
        bootstrap.getObjectMapper().registerModule(getHibernate4Module());
    }

    protected Hibernate4Module getHibernate4Module() {
        final Hibernate4Module module = new Hibernate4Module();
        module.disable(Hibernate4Module.Feature.USE_TRANSIENT_ANNOTATION);
        return module;
    }

    protected void configure(org.hibernate.cfg.Configuration configuration) {
    }

    /**
     * @return the sessionFactoryMap
     */
    public ImmutableMap<String, SessionFactory> getSessionFactoryMap() {
        return ImmutableMap.copyOf(sessionFactoryMap);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.codahale.dropwizard.ConfiguredBundle#run(java.lang.Object, com.codahale.dropwizard.setup.Environment)
     */
    @Override
    public final void run(T configuration, Environment environment) throws Exception {
        final Map<String, SessionFactory> sessionFactories = new LinkedHashMap<>();
        for (DataSourceRoute route : getDataSourceRoutes(configuration)) {
            final String routeKey = route.getRouteName();
            final DataSourceFactory dbConfig = route.getDatabase();

            final SessionFactory sessionFactory = sessionFactoryFactory.build(this, environment, dbConfig, entities,
                    routeKey);
            
            String validationQuery = "/* Sess Factory Health Check routeKey " + routeKey + " */ " + dbConfig.getValidationQuery();
            
            environment.healthChecks().register(
                    routeKey,
                    new SessionFactoryHealthCheck(environment.getHealthCheckExecutorService(),
                    		dbConfig.getValidationQueryTimeout().or(Duration.seconds(5)),
                    		sessionFactory, validationQuery));
            sessionFactories.put(routeKey, sessionFactory);
        }

        this.sessionFactoryMap = ImmutableMap.copyOf(sessionFactories);
        environment.jersey().register(new RoutingUnitOfWorkApplicationListener(this.sessionFactoryMap));
    }
}
