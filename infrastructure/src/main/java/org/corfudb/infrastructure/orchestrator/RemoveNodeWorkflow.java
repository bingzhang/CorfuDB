package org.corfudb.infrastructure.orchestrator;

import lombok.Getter;
import org.corfudb.protocols.wireprotocol.orchestrator.RemoveNodeRequest;
import org.corfudb.protocols.wireprotocol.orchestrator.Request;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.corfudb.format.Types.OrchestratorRequestType.REMOVE_NODE;

/**
 * @author Maithem
 */
public class RemoveNodeWorkflow extends Workflow {

    final RemoveNodeRequest request;

    @Getter
    final UUID id;

    public RemoveNodeWorkflow(Request request) {
        this.id = UUID.randomUUID();
        this.request = (RemoveNodeRequest) request;
    }

    @Override
    public String getName() {
        return REMOVE_NODE.toString();
    }

    @Override
    public List<Action> getActions() {
        return Arrays.asList(new RemoveNode());
    }

    class RemoveNode extends Action {
        @Override
        public String getName() {
            return "RemoveNode";
        }

        @Override
        public void impl(@Nonnull CorfuRuntime runtime) throws Exception {
            changeStatus(ActionStatus.STARTED);
            Layout layout = (Layout) runtime.getLayoutView().getLayout().clone();
            runtime.getLayoutManagementView().removeNode(layout,
                    request.getEndpoint());
            changeStatus(ActionStatus.COMPLETED);
        }
    }

}
