package io.servicefabric.cluster;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;


public class ClusterTest {

    @Test
    public void testJoin() throws Exception {
        List<ListenableFuture<ICluster>> nodes = new ArrayList<>();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            nodes.add(Cluster.join());
        }
        ListenableFuture<List<ICluster>> result = Futures.allAsList(nodes);
        try {
            result.get();
        } catch (InterruptedException | ExecutionException e) {
            fail();
        }
        long end = System.currentTimeMillis();
        System.out.println("Time: " + (end - start));
    }
}