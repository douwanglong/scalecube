package io.servicefabric.cluster;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.getRootCause;
import static com.google.common.util.concurrent.Futures.*;

import com.google.common.util.concurrent.*;
import io.servicefabric.cluster.fdetector.FailureDetector;
import io.servicefabric.cluster.gossip.GossipProtocol;
import io.servicefabric.cluster.gossip.IGossipProtocol;
import io.servicefabric.transport.Message;
import io.servicefabric.transport.Transport;
import io.servicefabric.transport.TransportAddress;
import io.servicefabric.transport.TransportEndpoint;

import com.google.common.base.Function;
import com.google.common.base.Throwables;

import io.servicefabric.transport.utils.IpAddressResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.Observable;
import rx.schedulers.Schedulers;

import java.net.BindException;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Main ICluster implementation.
 * 
 * @author Anton Kharenko
 */

public final class Cluster implements ICluster {

  private static final Logger LOGGER = LoggerFactory.getLogger(Cluster.class);

  private enum State {
    INSTANTIATED, JOINING, JOINED, LEAVING, STOPPED
  }

  // Cluster config
  private final String memberId;
  private final ClusterConfiguration config;

  // Cluster components
  private final Transport transport;
  private final FailureDetector failureDetector;
  private final GossipProtocol gossipProtocol;
  private final ClusterMembership clusterMembership;

  // Cluster state
  private final AtomicReference<State> state;

  private Cluster(ClusterConfiguration config) {
    checkNotNull(config);
    checkNotNull(config.transportSettings);
    checkNotNull(config.gossipProtocolSettings);
    checkNotNull(config.failureDetectorSettings);
    checkNotNull(config.clusterMembershipSettings);
    this.config = config;

    // Build local endpoint
    memberId = config.memberId != null ? config.memberId : UUID.randomUUID().toString();
    TransportEndpoint localTransportEndpoint = TransportEndpoint.from(memberId, TransportAddress.localTcp(config.port));

    // Build transport
    transport = Transport.newInstance(localTransportEndpoint, config.transportSettings);

    // Build gossip protocol component
    gossipProtocol = new GossipProtocol(localTransportEndpoint);
    gossipProtocol.setTransport(transport);
    gossipProtocol.setMaxGossipSent(config.gossipProtocolSettings.getMaxGossipSent());
    gossipProtocol.setGossipTime(config.gossipProtocolSettings.getGossipTime());
    gossipProtocol.setMaxEndpointsToSelect(config.gossipProtocolSettings.getMaxEndpointsToSelect());

    // Build failure detector component
    failureDetector = new FailureDetector(localTransportEndpoint, Schedulers.from(transport.getEventExecutor()));
    failureDetector.setTransport(transport);
    failureDetector.setPingTime(config.failureDetectorSettings.getPingTime());
    failureDetector.setPingTimeout(config.failureDetectorSettings.getPingTimeout());
    failureDetector.setMaxEndpointsToSelect(config.failureDetectorSettings.getMaxEndpointsToSelect());

    // Build cluster membership component
    clusterMembership = new ClusterMembership(localTransportEndpoint, Schedulers.from(transport.getEventExecutor()));
    clusterMembership.setFailureDetector(failureDetector);
    clusterMembership.setGossipProtocol(gossipProtocol);
    clusterMembership.setTransport(transport);
    clusterMembership.setLocalMetadata(config.metadata);
    clusterMembership.setSeedMembers(config.seedMembers);
    clusterMembership.setSyncTime(config.clusterMembershipSettings.getSyncTime());
    clusterMembership.setSyncTimeout(config.clusterMembershipSettings.getSyncTimeout());
    clusterMembership.setMaxSuspectTime(config.clusterMembershipSettings.getMaxSuspectTime());
    clusterMembership.setMaxShutdownTime(config.clusterMembershipSettings.getMaxShutdownTime());
    clusterMembership.setSyncGroup(config.clusterMembershipSettings.getSyncGroup());

    // Initial state
    this.state = new AtomicReference<>(State.INSTANTIATED);
    LOGGER.info("Cluster instance '{}' created with configuration: {}", memberId, config);
  }

  public static ICluster joinAwait() {
    return joinAwait(ClusterConfiguration.newInstance());
  }

  public static ICluster joinAwait(int port) {
    return joinAwait(ClusterConfiguration.newInstance().port(port));
  }

  public static ICluster joinAwait(int port, String seedMembers) {
    return joinAwait(ClusterConfiguration.newInstance().port(port).seedMembers(seedMembers));
  }
  public static ICluster joinAwait(String seedMembers) {
    return joinAwait(ClusterConfiguration.newInstance().seedMembers(seedMembers));
  }

  public static ICluster joinAwait(String memberId, int port, String seedMembers) {
    return joinAwait(ClusterConfiguration.newInstance().memberId(memberId).port(port).seedMembers(seedMembers));
  }

  public static ICluster joinAwait(ClusterConfiguration config) {
    return new Cluster(config).joinAwait0();
  }


  public static ListenableFuture<ICluster> join() {
    return join(ClusterConfiguration.newInstance());
  }

  public static ListenableFuture<ICluster> join(int port) {
    return join(ClusterConfiguration.newInstance().port(port));
  }

  public static ListenableFuture<ICluster> join(int port, String seedMembers) {
    return join(ClusterConfiguration.newInstance().port(port).seedMembers(seedMembers));
  }
  public static ListenableFuture<ICluster> join(String seedMembers) {
    return join(ClusterConfiguration.newInstance().seedMembers(seedMembers));
  }

  public static ListenableFuture<ICluster> join(String memberId, int port, String seedMembers) {
    return join(ClusterConfiguration.newInstance().memberId(memberId).port(port).seedMembers(seedMembers));
  }

  public static ListenableFuture<ICluster> join(final ClusterConfiguration config) {
    final ListenableFuture<ICluster> future = new Cluster(config).join0();
    return withFallback(future, new FutureFallback<ICluster>() {
      @Override
      public ListenableFuture<ICluster> create(@Nonnull Throwable throwable) throws Exception {
        if(throwable instanceof BindException){
          int port = config.port + 1;
          while(!IpAddressResolver.available(port)){
            port++;
          }
          return join(port);
        }
        return Futures.immediateFailedFuture(throwable);
      }
    });
  }

  @Override
  public void send(ClusterMember member, Message message) {
    checkJoinedState();
    transport.send(member.endpoint(), message);
  }

  @Override
  public void send(ClusterMember member, Message message, SettableFuture<Void> promise) {
    checkJoinedState();
    transport.send(member.endpoint(), message, promise);
  }

  @Override
  public Observable<Message> listen() {
    checkJoinedState();
    return transport.listen();
  }

  @Override
  public IGossipProtocol gossip() {
    checkJoinedState();
    return gossipProtocol;
  }

  @Override
  public IClusterMembership membership() {
    checkJoinedState();
    return clusterMembership;
  }

   private ListenableFuture<ICluster> join0() {
    updateClusterState(State.INSTANTIATED, State.JOINING);
    LOGGER.info("Cluster instance '{}' joining seed members: {}", memberId, config.seedMembers);
    ListenableFuture<Void> transportFuture = transport.start();
    ListenableFuture<Void> clusterFuture = transform(transportFuture, new AsyncFunction<Void, Void>() {
      @Override
      public ListenableFuture<Void> apply(@Nullable Void param) throws Exception {
        failureDetector.start();
        gossipProtocol.start();
        return clusterMembership.start();
      }
    });

    return transform(clusterFuture, new Function<Void, ICluster>() {
      @Override
      public ICluster apply(@Nullable Void param) {
        updateClusterState(State.JOINING, State.JOINED);
        LOGGER.info("Cluster instance '{}' joined cluster of members: {}", memberId, membership().members());
        return Cluster.this;
      }
    });


  }

  private ICluster joinAwait0() {
    try {
      return join0().get();
    } catch (Exception e) {
      throw Throwables.propagate(getRootCause(e));
    }
  }

  @Override
  public ListenableFuture<Void> leave() {
    updateClusterState(State.JOINED, State.LEAVING);
    LOGGER.info("Cluster instance '{}' leaving cluster", memberId);

    // Notify cluster members about graceful shutdown of current member
    clusterMembership.leave();

    // Wait for some time until 'leave' gossip start to spread through the cluster before stopping cluster components
    final SettableFuture<Void> transportStoppedFuture = SettableFuture.create();
    final ScheduledExecutorService stopExecutor = Executors.newSingleThreadScheduledExecutor();
    long delay = 3 * gossipProtocol.getGossipTime(); // wait for 3 gossip periods before stopping
    stopExecutor.schedule(new Runnable() {
      @Override
      public void run() {
        clusterMembership.stop();
        gossipProtocol.stop();
        failureDetector.stop();
        transport.stop(transportStoppedFuture);
      }
    }, delay, TimeUnit.MILLISECONDS);

    // Update cluster state to terminal state
    return transform(transportStoppedFuture, new Function<Void, Void>() {
      @Nullable
      @Override
      public Void apply(Void input) {
        stopExecutor.shutdown();
        updateClusterState(State.LEAVING, State.STOPPED);
        LOGGER.info("Cluster instance '{}' stopped", memberId);
        return input;
      }
    });
  }

  private void checkJoinedState() {
    State currentState = state.get();
    checkState(currentState == State.JOINED, "Illegal operation at state %s. Member should be joined to cluster.",
        state.get());
  }

  private void updateClusterState(State expected, State update) {
    boolean stateUpdated = state.compareAndSet(expected, update);
    checkState(stateUpdated, "Illegal state transition from %s to %s cluster state. Expected state %s.", state.get(),
        update, expected);
  }

}
