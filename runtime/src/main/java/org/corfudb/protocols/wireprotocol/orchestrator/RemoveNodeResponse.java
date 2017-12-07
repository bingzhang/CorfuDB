package org.corfudb.protocols.wireprotocol.orchestrator;

import lombok.Getter;
import org.corfudb.format.Types;

import static org.corfudb.format.Types.OrchestratorResponseType.DELETE_STATUS;
import static org.corfudb.format.Types.OrchestratorResponseType.WORKFLOW_ID;

/**
 * @author Maithem
 */
public class RemoveNodeResponse implements Response {

    @Getter
    boolean deleted;

    public RemoveNodeResponse(boolean deleted) {
        this.deleted = deleted;
    }

    public RemoveNodeResponse(byte[] buf) {
        if (buf[0] == 0) {
            this.deleted = false;
        } else {
            this.deleted = true;
        }
    }

    @Override
    public Types.OrchestratorResponseType getType() {
        return DELETE_STATUS;
    }

    @Override
    public byte[] getSerialized() {
        byte[] buf = new byte[1];
        if (deleted) {
            buf[0] = 1;
        } else {
            buf[0] = 0;
        }
        return buf;
    }
}
