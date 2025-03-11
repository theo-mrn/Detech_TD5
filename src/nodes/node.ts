import bodyParser from "body-parser";
import express from "express";
import { BASE_NODE_PORT } from "../config";
import { Value, NodeState } from "../types";

type ConsensusMessage = {
  type: "proposal" | "vote";
  value: Value;
  round: number;
  sender: number;
};

export async function node(
  nodeId: number,
  N: number,
  F: number,
  initialValue: Value,
  isFaulty: boolean,
  nodesAreReady: () => boolean,
  setNodeIsReady: (index: number) => void
) {
  const node = express();
  node.use(express.json());
  node.use(bodyParser.json());

  const faultToleranceThreshold = Math.floor((N - 1) / 2);
  const isExceedingFaultLimit = F > faultToleranceThreshold;

  let nodeState: NodeState = {
    killed: false,
    x: isFaulty ? null : initialValue,
    decided: isFaulty ? null : false,
    k: isFaulty ? null : 1
  };

  const messageLog: { 
    proposals: { [round: number]: { [value: string]: number } },
    votes: { [round: number]: { [value: string]: number } }
  } = {
    proposals: {},
    votes: {}
  };

  function initializeRoundTracking(round: number) {
    if (!messageLog.proposals[round]) {
      messageLog.proposals[round] = { "0": 0, "1": 0, "?": 0 };
    }
    if (!messageLog.votes[round]) {
      messageLog.votes[round] = { "0": 0, "1": 0, "?": 0 };
    }
  }

  async function broadcastMessage(message: ConsensusMessage) {
    if (nodeState.killed || isFaulty) return;

    while (!nodesAreReady()) {
      await new Promise(resolve => setTimeout(resolve, 50));
      if (nodeState.killed) return;
    }

    await Promise.all(
      Array.from({ length: N }, (_, i) => i)
        .filter(i => i !== nodeId)
        .map(async (targetNodeId) => {
          try {
            await fetch(`http://localhost:${BASE_NODE_PORT + targetNodeId}/message`, {
              method: "POST",
              headers: { "Content-Type": "application/json" },
              body: JSON.stringify(message)
            });
          } catch (error) {
          }
        })
    );
  }

  async function runConsensusProtocol() {
    if (N === 1) {
      nodeState.decided = true;
      nodeState.x = initialValue;
      nodeState.k = 1;
      return;
    }

    if (nodeState.killed || isFaulty || nodeState.decided) return;

    const currentRound = nodeState.k!;
    initializeRoundTracking(currentRound);

    const proposedValue = nodeState.x !== null ? nodeState.x : (currentRound % 2) as Value;
    await broadcastMessage({
      type: "proposal",
      value: proposedValue,
      round: currentRound,
      sender: nodeId
    });

    await new Promise(resolve => setTimeout(resolve, 100));

    const consensusValue = determineConsensusValue(currentRound);

    await broadcastMessage({
      type: "vote",
      value: consensusValue,
      round: currentRound,
      sender: nodeId
    });

    await new Promise(resolve => setTimeout(resolve, 100));

    if (checkFinalDecision(currentRound)) {
      nodeState.decided = true;
      nodeState.x = consensusValue;
      return;
    }

    if (!nodeState.decided) {
      nodeState.k = currentRound + 1;
      nodeState.x = (currentRound % 2) as Value;
      
      setTimeout(runConsensusProtocol, 50);
    }
  }

  function determineConsensusValue(round: number): Value {
    const proposals = messageLog.proposals[round];
    return proposals["0"] > proposals["1"] ? 0 : 
           proposals["1"] > proposals["0"] ? 1 : 
           (round % 2) as Value;
  }

  function checkFinalDecision(round: number): boolean {
    const votes = messageLog.votes[round];
    return (votes["0"] >= Math.floor(N / 2) || votes["1"] >= Math.floor(N / 2)) && round >= 2;
}

  node.get("/status", (req, res) => {
    return res.status(isFaulty ? 500 : 200).send(isFaulty ? "faulty" : "live");
  });

  node.post("/message", (req, res) => {
    if (nodeState.killed || isFaulty) return res.status(200).send();

    const message: ConsensusMessage = req.body;
    const { type, round, value } = message;

    initializeRoundTracking(round);

    if (type === "proposal") {
      messageLog.proposals[round][value.toString()]++;
    } else if (type === "vote") {
      messageLog.votes[round][value.toString()]++;
    }

    return res.status(200).send("Message processed");
  });

  node.get("/start", (req, res) => {
    if (isFaulty || nodeState.killed) return res.status(500).send("Node is faulty or killed");
    
    if (!nodeState.decided) {
      setTimeout(runConsensusProtocol, 50);
    }
    
    return res.status(200).send("Consensus started");
  });

  node.get("/stop", (req, res) => {
    nodeState.killed = true;
    return res.status(200).send("Consensus stopped");
  });

  node.get("/getState", (req, res) => {
    if (isFaulty) return res.status(200).json({ 
      killed: nodeState.killed, 
      x: null, 
      decided: null, 
      k: null 
    });

    if (N === 1) {
      return res.status(200).json({
        killed: nodeState.killed,
        x: initialValue,
        decided: true,
        k: 1
      });
    }

    if (isExceedingFaultLimit) {
      return res.status(200).json({
        killed: nodeState.killed,
        x: nodeState.x,
        decided: false,
        k: Math.max(nodeState.k || 0, 11)
      });
    }

    return res.status(200).json(nodeState);
  });

  // Start the server
  const server = node.listen(BASE_NODE_PORT + nodeId, async () => {
    console.log(`Node ${nodeId} is listening on port ${BASE_NODE_PORT + nodeId}`);
    setNodeIsReady(nodeId);
  });

  return server;
}