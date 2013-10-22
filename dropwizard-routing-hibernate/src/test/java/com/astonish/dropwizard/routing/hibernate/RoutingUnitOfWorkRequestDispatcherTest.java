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

import com.astonish.dropwizard.routing.db.RouteStore;
import com.codahale.dropwizard.hibernate.UnitOfWork;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.sun.jersey.api.NotFoundException;
import com.sun.jersey.api.core.HttpContext;
import com.sun.jersey.api.core.HttpRequestContext;
import com.sun.jersey.spi.dispatch.RequestDispatcher;
import org.hibernate.*;
import org.hibernate.context.internal.ManagedSessionContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.fest.assertions.api.Assertions.failBecauseExceptionWasNotThrown;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ RouteStore.class })
public class RoutingUnitOfWorkRequestDispatcherTest {
    private final UnitOfWork unitOfWork = mock(UnitOfWork.class);
    private final RequestDispatcher underlying = mock(RequestDispatcher.class);
    private final SessionFactory sessionFactory = mock(SessionFactory.class);
    private final RoutingUnitOfWorkRequestDispatcher dispatcher = new RoutingUnitOfWorkRequestDispatcher(unitOfWork,
            underlying, ImmutableMap.<Optional<String>, SessionFactory> builder()
                    .put(Optional.<String> absent(), sessionFactory).build());

    private final Object resource = mock(Object.class);
    private final HttpContext context = mock(HttpContext.class);
    private final HttpRequestContext requestContext = mock(HttpRequestContext.class);
    private final Session session = mock(Session.class);
    private final Transaction transaction = mock(Transaction.class);
    private final RouteStore routeStore = mock(RouteStore.class);

    @Before
    public void setUp() throws Exception {
        when(unitOfWork.readOnly()).thenReturn(false);
        when(unitOfWork.cacheMode()).thenReturn(CacheMode.NORMAL);
        when(unitOfWork.flushMode()).thenReturn(FlushMode.AUTO);
        when(unitOfWork.transactional()).thenReturn(true);

        when(sessionFactory.openSession()).thenReturn(session);
        when(session.getSessionFactory()).thenReturn(sessionFactory);
        when(session.beginTransaction()).thenReturn(transaction);
        when(session.getTransaction()).thenReturn(transaction);

        when(context.getRequest()).thenReturn(requestContext);

        when(transaction.isActive()).thenReturn(true);

        mockStatic(RouteStore.class);
        when(RouteStore.getInstance()).thenReturn(routeStore);
    }

    @Test
    public void hasAUnitOfWork() throws Exception {
        assertThat(dispatcher.getUnitOfWork()).isEqualTo(unitOfWork);
    }

    @Test
    public void hasADispatcher() throws Exception {
        assertThat(dispatcher.getDispatcher()).isEqualTo(underlying);
    }

    @Test
    public void hasASessionFactory() throws Exception {
        assertThat(dispatcher.getSessionFactoryMap().entrySet().iterator().next().getValue()).isEqualTo(sessionFactory);
    }

    @Test
    public void opensAndClosesASession() throws Exception {
        dispatcher.dispatch(resource, context);

        final InOrder inOrder = inOrder(sessionFactory, session, underlying);
        inOrder.verify(sessionFactory).openSession();
        inOrder.verify(underlying).dispatch(resource, context);
        inOrder.verify(session).close();
    }

    @Test
    public void bindsAndUnbindsTheSessionToTheManagedContext() throws Exception {
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                assertThat(ManagedSessionContext.hasBind(sessionFactory)).isTrue();
                return null;
            }
        }).when(underlying).dispatch(resource, context);

        dispatcher.dispatch(resource, context);

        assertThat(ManagedSessionContext.hasBind(sessionFactory)).isFalse();
    }

    @Test
    public void configuresTheSessionsReadOnlyDefault() throws Exception {
        when(unitOfWork.readOnly()).thenReturn(true);

        dispatcher.dispatch(resource, context);

        verify(session).setDefaultReadOnly(true);
    }

    @Test
    public void configuresTheSessionsCacheMode() throws Exception {
        when(unitOfWork.cacheMode()).thenReturn(CacheMode.IGNORE);

        dispatcher.dispatch(resource, context);

        verify(session).setCacheMode(CacheMode.IGNORE);
    }

    @Test
    public void configuresTheSessionsFlushMode() throws Exception {
        when(unitOfWork.flushMode()).thenReturn(FlushMode.ALWAYS);

        dispatcher.dispatch(resource, context);

        verify(session).setFlushMode(FlushMode.ALWAYS);
    }

    @Test
    public void doesNotBeginATransactionIfNotTransactional() throws Exception {
        when(unitOfWork.transactional()).thenReturn(false);
        when(session.getTransaction()).thenReturn(null);

        dispatcher.dispatch(resource, context);

        verify(session, never()).beginTransaction();
        verifyZeroInteractions(transaction);
    }

    @Test
    public void beginsAndCommitsATransactionIfTransactional() throws Exception {
        dispatcher.dispatch(resource, context);

        final InOrder inOrder = inOrder(session, transaction, underlying);
        inOrder.verify(session).beginTransaction();
        inOrder.verify(underlying).dispatch(resource, context);
        inOrder.verify(transaction).commit();
        inOrder.verify(session).close();
    }

    @Test
    public void rollsBackTheTransactionOnException() throws Exception {
        doThrow(new RuntimeException("OH NO")).when(underlying).dispatch(resource, context);

        try {
            dispatcher.dispatch(resource, context);
            failBecauseExceptionWasNotThrown(RuntimeException.class);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).isEqualTo("OH NO");
        }

        final InOrder inOrder = inOrder(session, transaction, underlying);
        inOrder.verify(session).beginTransaction();
        inOrder.verify(underlying).dispatch(resource, context);
        inOrder.verify(transaction).rollback();
        inOrder.verify(session).close();
    }

    @Test
    public void doesNotCommitAnInactiveTransaction() throws Exception {
        when(transaction.isActive()).thenReturn(false);

        dispatcher.dispatch(resource, context);

        verify(transaction, never()).commit();
    }

    @Test
    public void doesNotCommitANullTransaction() throws Exception {
        when(session.getTransaction()).thenReturn(null);

        dispatcher.dispatch(resource, context);

        verify(transaction, never()).commit();
    }

    @Test
    public void doesNotRollbackAnInactiveTransaction() throws Exception {
        when(transaction.isActive()).thenReturn(false);

        doThrow(new RuntimeException("OH NO")).when(underlying).dispatch(resource, context);

        try {
            dispatcher.dispatch(resource, context);
            failBecauseExceptionWasNotThrown(RuntimeException.class);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).isEqualTo("OH NO");
        }

        verify(transaction, never()).rollback();
    }

    @Test
    public void doesNotRollbackANullTransaction() throws Exception {
        when(session.getTransaction()).thenReturn(null);

        doThrow(new RuntimeException("OH NO")).when(underlying).dispatch(resource, context);

        try {
            dispatcher.dispatch(resource, context);
            failBecauseExceptionWasNotThrown(RuntimeException.class);
        } catch (RuntimeException e) {
            assertThat(e.getMessage()).isEqualTo("OH NO");
        }

        verify(transaction, never()).rollback();
    }

    @Test
    public void routable() {
        final SessionFactory shouldNotBeUsed = mock(SessionFactory.class);

        final ImmutableMap.Builder<Optional<String>, SessionFactory> bldr = new ImmutableMap.Builder<>();
        bldr.put(Optional.<String> absent(), shouldNotBeUsed);
        bldr.put(Optional.of("route"), sessionFactory);
        final RoutingUnitOfWorkRequestDispatcher routableRequestDispatcher = new RoutingUnitOfWorkRequestDispatcher(
                unitOfWork, underlying, bldr.build());

        when(routeStore.getRoute()).thenReturn("route");
        routableRequestDispatcher.dispatch(resource, context);

        final InOrder inOrder = inOrder(sessionFactory, session, underlying);
        inOrder.verify(sessionFactory).openSession();
        inOrder.verify(underlying).dispatch(resource, context);
        inOrder.verify(session).close();

        verifyZeroInteractions(shouldNotBeUsed);
    }

    @Test(expected = NotFoundException.class)
    public void invalidRoute() {
        final SessionFactory shouldNotBeUsed = mock(SessionFactory.class);

        final ImmutableMap.Builder<Optional<String>, SessionFactory> bldr = new ImmutableMap.Builder<>();
        bldr.put(Optional.<String> absent(), shouldNotBeUsed);
        bldr.put(Optional.of("route"), sessionFactory);
        final RoutingUnitOfWorkRequestDispatcher routableRequestDispatcher = new RoutingUnitOfWorkRequestDispatcher(
                unitOfWork, underlying, bldr.build());

        when(routeStore.getRoute()).thenReturn("invalid");
        routableRequestDispatcher.dispatch(resource, context);
    }
}