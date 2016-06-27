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
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.hibernate.SessionFactoryHealthCheck;
import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

import com.astonish.dropwizard.routing.db.DataSourceRoute;
import com.astonish.dropwizard.routing.hibernate.RoutingHibernateBundle;
import com.astonish.dropwizard.routing.hibernate.RoutingSessionFactoryFactory;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.hibernate4.Hibernate4Module;
import com.google.common.collect.ImmutableList;

import org.hibernate.SessionFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class RoutingHibernateBundleTest {
    private static final String ROUTE_ONE = "RouteOne";
    private static final String ROUTE_TWO = "RouteTwo";
    private final DataSourceFactory dbConfigRouteOne = new DataSourceFactory();
    private final DataSourceFactory dbConfigRouteTwo = new DataSourceFactory();
    private final SessionFactory sessionFactoryRouteOne = mock(SessionFactory.class);
    private final SessionFactory sessionFactoryRouteTwo = mock(SessionFactory.class);
    private final ImmutableList<Class<?>> entities = ImmutableList.<Class<?>> of(Person.class);
    private final RoutingSessionFactoryFactory factory = mock(RoutingSessionFactoryFactory.class);
    private final Configuration configuration = mock(Configuration.class);
    private final HealthCheckRegistry healthChecks = mock(HealthCheckRegistry.class);
    private final JerseyEnvironment jerseyEnvironment = mock(JerseyEnvironment.class);
    private final Environment environment = mock(Environment.class);
    private final RoutingHibernateBundle<Configuration> bundle = new RoutingHibernateBundle<Configuration>(entities,
            factory) {
        @Override
        public ImmutableList<DataSourceRoute> getDataSourceRoutes(Configuration configuration) {
            final ImmutableList.Builder<DataSourceRoute> bldr = new ImmutableList.Builder<>();

            final DataSourceRoute dsRouteOne = new DataSourceRoute();
            dsRouteOne.setDatabase(dbConfigRouteOne);
            dsRouteOne.setRouteName(ROUTE_ONE);
            bldr.add(dsRouteOne);

            final DataSourceRoute dsRouteTwo = new DataSourceRoute();
            dsRouteTwo.setDatabase(dbConfigRouteTwo);
            dsRouteTwo.setRouteName(ROUTE_TWO);
            bldr.add(dsRouteTwo);
            return bldr.build();
        }
    };

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        when(environment.healthChecks()).thenReturn(healthChecks);
        when(environment.jersey()).thenReturn(jerseyEnvironment);

        when(factory.build(eq(bundle), any(Environment.class), eq(dbConfigRouteOne), anyList(), eq(ROUTE_ONE)))
                .thenReturn(sessionFactoryRouteOne);
        when(factory.build(eq(bundle), any(Environment.class), eq(dbConfigRouteTwo), anyList(), eq(ROUTE_TWO)))
                .thenReturn(sessionFactoryRouteTwo);
    }

    @Test
    public void addsHibernateSupportToJackson() throws Exception {
        final ObjectMapper objectMapperFactory = mock(ObjectMapper.class);

        final Bootstrap<?> bootstrap = mock(Bootstrap.class);
        when(bootstrap.getObjectMapper()).thenReturn(objectMapperFactory);

        bundle.initialize(bootstrap);

        final ArgumentCaptor<Module> captor = ArgumentCaptor.forClass(Module.class);
        verify(objectMapperFactory).registerModule(captor.capture());

        assertThat(captor.getValue()).isInstanceOf(Hibernate4Module.class);
    }

    @Test
    public void buildsSessionFactories() throws Exception {
        bundle.run(configuration, environment);

        verify(factory).build(bundle, environment, dbConfigRouteOne, entities, ROUTE_ONE);
        verify(factory).build(bundle, environment, dbConfigRouteTwo, entities, ROUTE_TWO);
    }

    @Test
    public void registersSessionFactoryHealthChecks() throws Exception {
        dbConfigRouteOne.setValidationQuery("SELECT something RouteOne");
        dbConfigRouteTwo.setValidationQuery("SELECT something RouteTwo");

        bundle.run(configuration, environment);

        final ArgumentCaptor<SessionFactoryHealthCheck> captor = ArgumentCaptor
                .forClass(SessionFactoryHealthCheck.class);
        verify(healthChecks).register(eq(ROUTE_ONE), captor.capture());
        assertThat(captor.getValue().getSessionFactory()).isEqualTo(sessionFactoryRouteOne);
        assertThat(captor.getValue().getValidationQuery()).isEqualTo("/* Sess Factory Health Check routeKey RouteOne */ SELECT something RouteOne");

        verify(healthChecks).register(eq(ROUTE_TWO), captor.capture());
        assertThat(captor.getValue().getSessionFactory()).isEqualTo(sessionFactoryRouteTwo);
        assertThat(captor.getValue().getValidationQuery()).isEqualTo("/* Sess Factory Health Check routeKey RouteTwo */ SELECT something RouteTwo");
    }

    @Test
    public void registersATransactionalAdapter() throws Exception {
        bundle.run(configuration, environment);

        final ArgumentCaptor<RoutingUnitOfWorkApplicationListener> captor = ArgumentCaptor
                .forClass(RoutingUnitOfWorkApplicationListener.class);
        verify(jerseyEnvironment).register(captor.capture());

        assertThat(captor.getValue().getSessionFactoryMap()).containsValue(sessionFactoryRouteOne);
        assertThat(captor.getValue().getSessionFactoryMap()).containsValue(sessionFactoryRouteTwo);
    }
}
