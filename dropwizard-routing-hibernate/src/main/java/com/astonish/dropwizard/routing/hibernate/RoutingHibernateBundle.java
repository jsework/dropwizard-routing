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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;

import com.astonish.dropwizard.routing.db.DataSourceRoute;
import com.astonish.dropwizard.routing.db.RoutingDatabaseConfiguration;
import com.fasterxml.jackson.datatype.hibernate4.Hibernate4Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import org.hibernate.SessionFactory;

/**
 * Routing Hibernate bundle.  Seriously.
 */
public abstract class RoutingHibernateBundle<T extends Configuration> implements ConfiguredBundle<T>,
        RoutingDatabaseConfiguration<T> {
    private ImmutableMap<String, SessionFactory> sessionFactoryMap;
    private final ImmutableList<Class<?>> entities;
    private final RoutingSessionFactoryFactory sessionFactoryFactory;

    private ListeningExecutorService executorService;
    private CountDownLatch startSignal;
    
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
        this.startSignal = new CountDownLatch(1);
        this.executorService = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(100)); // TODO make thread quantity configurable configuration.getAms360DebugNumberOfThreads()));
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
        this.sessionFactoryMap = ImmutableMap.copyOf(buildSessionFactories(configuration, environment));
        environment.jersey().register(new RoutingUnitOfWorkApplicationListener(this.sessionFactoryMap));
    }
    
    private Map<String, SessionFactory> buildSessionFactories(T configuration, Environment environment) throws Exception {
    	    	
        final Map<String, SessionFactory> sessionFactories = new LinkedHashMap<>();
        
        List<List<DataSourceRoute>> chunks = Lists.partition(getDataSourceRoutes(configuration), 14); // todo configure the chunk size
        int chunkIndex = 0;
        
        for (List<DataSourceRoute> chunk : chunks) {
        	List<ListenableFuture<FutureCallResult>> listenableFutureList = new ArrayList<ListenableFuture<FutureCallResult>>();
        	
	        for (DataSourceRoute route : chunk) {	        	
	        	listenableFutureList.add(getAgencySessionFactory(route, environment));
	        }

	        ListenableFuture<List<FutureCallResult>> futureResults = Futures.allAsList(listenableFutureList);
	        
	        startSignal.countDown();	        
	        System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!********** start waiting, chunk index " + (chunkIndex++) );
		        
	        List<FutureCallResult> listOfFutureCallresults = futureResults.get();
        
	        System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!********** Done waiting, chunk index " + chunkIndex + " "+ listOfFutureCallresults.size() + " results");
	        
			for(FutureCallResult futureCallResult : listOfFutureCallresults){
				final String routeKey = futureCallResult.route.getRouteName();
				final DataSourceFactory dbConfig = futureCallResult.route.getDatabase();
	            
				String validationQuery = "/* Sess Factory Health Check routeKey " + routeKey + " */ " + dbConfig.getValidationQuery();
	
				environment.healthChecks().register(	routeKey,
														new SessionFactoryHealthCheck(environment.getHealthCheckExecutorService(),
																						dbConfig.getValidationQueryTimeout().or(Duration.seconds(5)),
																						futureCallResult.sessionFactory, validationQuery));			
				sessionFactories.put(routeKey, futureCallResult.sessionFactory);
	        }
        }
        
        System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!********** COMPLETELY Done waiting on all chunks");        
        return sessionFactories;
    }

    private ListenableFuture<FutureCallResult> getAgencySessionFactory(final DataSourceRoute route, final Environment environment) {
    	final RoutingHibernateBundle<T> routingHibernateBundle = this;
    	
    	return executorService.submit(new Callable<FutureCallResult>() {
    		@Override
    		public FutureCallResult call() throws Exception {
    			startSignal.await();
    			System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!********** Start Future Call for agency " + route.getRouteName());    			    			
    			final SessionFactory sessionFactory = sessionFactoryFactory.build(routingHibernateBundle, environment, route.getDatabase(), entities, route.getRouteName());
    			
    			FutureCallResult result = new FutureCallResult();
    			result.route = route;
    			result.sessionFactory = sessionFactory;

    			System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!********** End Future Call for agency " + route.getRouteName());
    			
    			return result;
    		}
    	});
    }
    
    class FutureCallResult {
    	DataSourceRoute route;
    	SessionFactory sessionFactory;
    }
}
