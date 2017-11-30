package org.corfudb.runtime.view;

import java.util.List;
import java.util.UUID;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.orchestrator.AddNodeResponse;
import org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.util.CFUtils;

/**
 * A view of the Management Server to manage reconfigurations of the Corfu Cluster.
 *
 * <p>Created by zlokhandwala on 11/20/17.</p>
 */
@Slf4j
public class ManagementView extends AbstractView {

    private final long layoutRefreshTimeout = 500;

    public ManagementView(@NonNull CorfuRuntime runtime) {
        super(runtime);
    }

    /**
     * Add a new node to the existing cluster.
     *
     * @param endpoint Endpoint of the new node to be added to the cluster.
     * @return True if completed successfully.
     */
    public boolean addNode(String endpoint) {
        return layoutHelper(l -> {

            // Choosing the tail log server to run the workflow to optimize bulk reads on the
            // same node.
            List<String> logServers = l.getSegments().get(0).getStripes().get(0).getLogServers();
            String server = logServers.get(logServers.size() - 1);

            OrchestratorResponse response = CFUtils.getUninterruptibly(runtime
                    .getRouter(server)
                    .getClient(ManagementClient.class)
                    .addNodeRequest(endpoint));
            UUID workflowId = ((AddNodeResponse) response.getResponse()).getWorkflowId();


            while (!runtime.getLayoutView().getLayout().getAllServers().contains(endpoint)
                    || runtime.getLayoutView().getLayout().getSegments().size() != 1) {
                runtime.invalidateLayout();
                try {
                    Thread.sleep(layoutRefreshTimeout);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            return true;
        });
    }
}
