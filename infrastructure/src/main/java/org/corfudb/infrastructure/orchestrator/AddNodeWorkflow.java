package org.corfudb.infrastructure.orchestrator;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.RangeWriteMsg;
import org.corfudb.protocols.wireprotocol.orchestrator.AddNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.Request;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.concurrent.NotThreadSafe;

import static org.corfudb.format.Types.OrchestratorRequestType.ADD_NODE;

/**
 * A definition of a workflow that adds a new node to the cluster.
 *
 * @author Maithem
 */
@NotThreadSafe
@Slf4j
public class AddNodeWorkflow implements Workflow {

    final AddNodeRequest request;

    private Layout newLayout;

    @Getter
    final UUID id;

    public AddNodeWorkflow(Request request) {
        this.id = UUID.randomUUID();
        this.request = (AddNodeRequest) request;
    }

    @Override
    public String getName() {
        return ADD_NODE.toString();
    }

    @Override
    public List<Action> getActions() {
        return Arrays.asList(new BootstrapNode(),
                new AddNodeToLayout(),
                new StateTransfer(),
                new MergeSegments());
    }

    class BootstrapNode extends Action {
        @Override
        public String getName() {
            return "BootstrapNode";
        }

        @Override
        public void impl(@Nonnull CorfuRuntime runtime) throws Exception {
            changeStatus(ActionStatus.STARTED);

            try {
                runtime.getLayoutManagementView().bootstrapNewNode(request.getEndpoint());
            } catch (Exception e) {
                if (e.getCause() instanceof AlreadyBootstrappedException) {
                    log.info("BootstrapNode: Node {} already bootstrapped, skipping.", request.getEndpoint());
                } else {
                    log.error("execute: Error during bootstrap", e);
                    changeStatus(ActionStatus.ERROR);
                }
            }

            changeStatus(ActionStatus.COMPLETED);
        }
    }


    /**
     * This action adds a new node to the layout. If it is also
     * added as a logunit server, then in addition to adding
     * the node the address space segment is split at the
     * tail determined during the layout modification.
     */
    class AddNodeToLayout extends Action {
        @Override
        public String getName() {
            return "AddNodeToLayout";
        }

        @Override
        public void impl(@Nonnull CorfuRuntime runtime) throws Exception {
            changeStatus(ActionStatus.STARTED);
            Layout currentLayout = (Layout) runtime.getLayoutView().getLayout().clone();

            if (currentLayout.getAllServers().contains(request.getEndpoint())) {
                log.info("Node {} already exists in the layout, skipping.", request.getEndpoint());
                newLayout = currentLayout;
                changeStatus(ActionStatus.COMPLETED);
                return;
            }

            runtime.getLayoutManagementView().addNode(currentLayout, request.getEndpoint(),
                    true, true,
                    true, false,
                    0);

            runtime.invalidateLayout();
            newLayout = (Layout) runtime.getLayoutView().getLayout().clone();
            changeStatus(ActionStatus.COMPLETED);
            return;

        }
    }

    /**
     * Transfer an address segment from a cluster to a new node. The epoch shouldn't change
     * during the segment transfer.
     * @param endpoint destination node
     * @param runtime The runtime to read the segment from
     * @param segment segment to transfer
     */
    public void stateTransfer(String endpoint, CorfuRuntime runtime,
                              Layout.LayoutSegment segment) throws Exception {

        final long chunkSize = 2500;

        long trimMark = runtime.getAddressSpaceView().getTrimMark();
        if (trimMark > segment.getEnd()) {
            log.info("stateTransfer: Nothing to transfer, trimMark {} greater than end of segment {}",
                    trimMark, segment.getEnd());
            return;
        }

        for (long chunkStart = segment.getStart(); chunkStart < segment.getEnd()
                ; chunkStart = chunkStart + chunkSize) {
            long chunkEnd = Math.min((chunkStart + chunkSize - 1), segment.getEnd() - 1);

            Map<Long, ILogData> dataMap = runtime.getAddressSpaceView()
                    .cacheFetch(ContiguousSet.create(
                            Range.closed(chunkStart, chunkEnd),
                            DiscreteDomain.longs()));

            List<LogData> entries = new ArrayList<>();
            for (long x = chunkStart; x <= chunkEnd; x++) {
                if (dataMap.get(x) == null) {
                    log.error("Missing address {} in range {}-{}", x, chunkStart, chunkEnd);
                    throw new IllegalStateException("Missing address");
                }
                entries.add((LogData) dataMap.get(x));
            }

            // Write segment chunk to the new logunit
            boolean transferSuccess = runtime
                    .getRouter(endpoint)
                    .getClient(LogUnitClient.class)
                    .writeRange(entries).get();

            if (!transferSuccess) {
                log.error("stateTransfer: Failed to transfer {}-{} to {}", chunkSize,
                        chunkEnd, endpoint);
                throw new IllegalStateException("Failed to transfer!");
            }

            log.info("stateTransfer: Transferred address chunk [{}, {}]",
                    chunkStart, chunkEnd);
        }
    }


    /**
     * Copies the split segment to the new node, if it
     * is the new node also participates as a logging unit.
     */
    class StateTransfer extends Action {
        @Override
        public String getName() {
            return "StateTransfer";
        }

        @Override
        public void impl(@Nonnull CorfuRuntime runtime) throws Exception {
            // Transfer the replicated segment to the new node
            stateTransfer(request.getEndpoint(), runtime, newLayout.getSegment(0));
        }
    }

    /**
     * Merges the fragmented segment if the AddNodeToLayout action caused any
     * segments to split
     */
    class MergeSegments extends Action {
        @Override
        public String getName() {
            return "MergeSegments";
        }

        @Override
        public void impl(@Nonnull CorfuRuntime runtime) throws Exception {
            // Transfer the replicated segment to the new node
            // TODO(Maithem) skip this step if there are no disjoint segments
            runtime.getLayoutManagementView().mergeSegments(newLayout);
        }
    }
}
