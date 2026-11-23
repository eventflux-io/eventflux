import type { Node, Edge, Connection } from '@xyflow/react';
import type { ElementType } from '../types';

/**
 * How many connections each element type may have, and to which types.
 * - maxInputs is the most incoming connections allowed. null means no limit.
 * - maxOutputs is the most outgoing connections allowed. null means no limit.
 * - canBeSource says whether a pipeline may start at this element.
 * - canBeSink says whether a pipeline may end at this element.
 *
 * validateConnection checks both sides. A connection must be in the source's
 * allowedTargets and in the target's allowedSources, when each list is set.
 */
interface CardinalityRule {
  maxInputs: number | null;
  maxOutputs: number | null;
  canBeSource: boolean;
  canBeSink: boolean;
  allowedSources?: ElementType[]; // element types that may connect into this one
  allowedTargets?: ElementType[]; // element types this one may connect into
}

const cardinalityRules: Record<ElementType, CardinalityRule> = {
  // External connectors
  source: {
    maxInputs: 0, // data comes from an external system
    maxOutputs: 1, // feeds at most one stream
    canBeSource: true,
    canBeSink: false,
    allowedTargets: ['stream'], // streams only
  },
  sink: {
    maxInputs: 1, // fed by at most one stream
    maxOutputs: 0, // data goes to an external system
    canBeSource: false,
    canBeSink: true,
    allowedSources: ['stream'], // the stream may itself be a query output
  },

  // Streams carry data both in and out
  stream: {
    maxInputs: null, // fed by a source or by queries that insert into it
    maxOutputs: null, // can feed many processing elements or sinks
    canBeSource: true,
    canBeSink: true, // can be the target of INSERT INTO
    allowedSources: ['source', 'window', 'filter', 'projection', 'aggregation', 'groupBy', 'join', 'pattern', 'partition'], // a source or query results
    allowedTargets: ['window', 'filter', 'projection', 'aggregation', 'groupBy', 'join', 'pattern', 'partition', 'sink', 'stream'], // 'stream' is listed, but allowedSources above rejects it
  },
  trigger: {
    maxInputs: 0,
    maxOutputs: null, // can drive many queries
    canBeSource: true,
    canBeSink: false,
    allowedTargets: ['window', 'filter', 'projection', 'aggregation', 'groupBy', 'stream'],
  },
  table: {
    maxInputs: null, // many queries can write to it
    maxOutputs: null, // many queries can read from it
    canBeSource: true,
    canBeSink: true,
    allowedTargets: ['window', 'filter', 'projection', 'aggregation', 'groupBy', 'join', 'stream'],
  },

  // Processing elements. Mostly one input and one output. They can write to a stream with INSERT INTO.
  window: {
    maxInputs: 1,
    maxOutputs: 1,
    canBeSource: false,
    canBeSink: false,
    allowedSources: ['stream', 'table', 'trigger'],
    allowedTargets: ['filter', 'projection', 'aggregation', 'groupBy', 'stream'],
  },
  filter: {
    maxInputs: 1,
    maxOutputs: 1,
    canBeSource: false,
    canBeSink: false,
    allowedSources: ['stream', 'table', 'trigger', 'window'],
    allowedTargets: ['window', 'filter', 'projection', 'aggregation', 'groupBy', 'join', 'stream'],
  },
  projection: {
    maxInputs: 1,
    maxOutputs: 1,
    canBeSource: false,
    canBeSink: false,
    allowedSources: ['stream', 'table', 'trigger', 'window', 'filter', 'aggregation', 'groupBy', 'join'],
    allowedTargets: ['filter', 'projection', 'stream'],
  },
  aggregation: {
    maxInputs: 1,
    maxOutputs: 1,
    canBeSource: false,
    canBeSink: false,
    allowedSources: ['stream', 'table', 'trigger', 'window', 'filter', 'groupBy'],
    allowedTargets: ['projection', 'filter', 'stream'],
  },
  groupBy: {
    maxInputs: 1,
    maxOutputs: 1,
    canBeSource: false,
    canBeSink: false,
    allowedSources: ['stream', 'table', 'trigger', 'window', 'filter'],
    allowedTargets: ['aggregation', 'projection', 'stream'],
  },

  // Special processing elements
  join: {
    maxInputs: 2, // left and right side. Only the upper limit is enforced.
    maxOutputs: 1,
    canBeSource: false,
    canBeSink: false,
    allowedSources: ['stream', 'table', 'window', 'filter'],
    allowedTargets: ['filter', 'projection', 'aggregation', 'groupBy', 'stream'],
  },
  pattern: {
    maxInputs: null, // a pattern can span many streams
    maxOutputs: 1,
    canBeSource: false,
    canBeSink: false,
    allowedSources: ['stream'],
    allowedTargets: ['filter', 'projection', 'stream'],
  },
  partition: {
    maxInputs: 1,
    maxOutputs: 1,
    canBeSource: false,
    canBeSink: false,
    allowedSources: ['stream', 'window', 'filter'],
    allowedTargets: ['window', 'filter', 'projection', 'aggregation', 'groupBy', 'stream'],
  },
};

export interface ValidationResult {
  valid: boolean;
  reason?: string;
}

/**
 * Checks whether a new connection is allowed by the rules above.
 * Returns valid false with a reason the UI can show when it is not.
 */
export function validateConnection(
  connection: Connection,
  nodes: Node[],
  edges: Edge[]
): ValidationResult {
  const sourceNode = nodes.find((n) => n.id === connection.source);
  const targetNode = nodes.find((n) => n.id === connection.target);

  if (!sourceNode || !targetNode) {
    return { valid: false, reason: 'Source or target node not found' };
  }

  const sourceType = sourceNode.type as ElementType;
  const targetType = targetNode.type as ElementType;

  const sourceRule = cardinalityRules[sourceType];
  const targetRule = cardinalityRules[targetType];

  if (!sourceRule || !targetRule) {
    return { valid: false, reason: 'Unknown element type' };
  }

  if (sourceRule.maxOutputs !== null) {
    const currentOutputs = edges.filter((e) => e.source === connection.source).length;
    if (currentOutputs >= sourceRule.maxOutputs) {
      return {
        valid: false,
        reason: `${getElementLabel(sourceType)} can only have ${sourceRule.maxOutputs} output connection${sourceRule.maxOutputs === 1 ? '' : 's'}`,
      };
    }
  }

  if (targetRule.maxInputs !== null) {
    const currentInputs = edges.filter((e) => e.target === connection.target).length;
    if (currentInputs >= targetRule.maxInputs) {
      return {
        valid: false,
        reason: `${getElementLabel(targetType)} can only have ${targetRule.maxInputs} input connection${targetRule.maxInputs === 1 ? '' : 's'}`,
      };
    }
  }

  if (sourceRule.allowedTargets && !sourceRule.allowedTargets.includes(targetType)) {
    return {
      valid: false,
      reason: `${getElementLabel(sourceType)} cannot connect to ${getElementLabel(targetType)}`,
    };
  }

  if (targetRule.allowedSources && !targetRule.allowedSources.includes(sourceType)) {
    return {
      valid: false,
      reason: `${getElementLabel(targetType)} cannot receive connections from ${getElementLabel(sourceType)}`,
    };
  }

  const duplicateConnection = edges.find(
    (e) => e.source === connection.source && e.target === connection.target
  );
  if (duplicateConnection) {
    return { valid: false, reason: 'Connection already exists' };
  }

  // A stream may have only ONE external connector, a Source or a Sink, not both
  if (sourceType === 'source' && targetType === 'stream') {
    const existingSourceConnection = edges.find((e) => {
      const edgeSourceNode = nodes.find((n) => n.id === e.source);
      return edgeSourceNode?.type === 'source' && e.target === connection.target;
    });
    if (existingSourceConnection) {
      return { valid: false, reason: 'Stream already has a Source connected.' };
    }
    const existingSinkConnection = edges.find((e) => {
      const edgeTargetNode = nodes.find((n) => n.id === e.target);
      return e.source === connection.target && edgeTargetNode?.type === 'sink';
    });
    if (existingSinkConnection) {
      return { valid: false, reason: 'Stream already connects to a Sink. A stream can have either a Source or a Sink, not both.' };
    }
  }

  if (sourceType === 'stream' && targetType === 'sink') {
    const existingSinkConnection = edges.find((e) => {
      const edgeTargetNode = nodes.find((n) => n.id === e.target);
      return e.source === connection.source && edgeTargetNode?.type === 'sink';
    });
    if (existingSinkConnection) {
      return { valid: false, reason: 'Stream already connects to a Sink.' };
    }
    const existingSourceConnection = edges.find((e) => {
      const edgeSourceNode = nodes.find((n) => n.id === e.source);
      return edgeSourceNode?.type === 'source' && e.target === connection.source;
    });
    if (existingSourceConnection) {
      return { valid: false, reason: 'Stream already has a Source connected. A stream can have either a Source or a Sink, not both.' };
    }
  }

  if (connection.source === connection.target) {
    return { valid: false, reason: 'Cannot connect element to itself' };
  }

  return { valid: true };
}

/**
 * Display label for an element type. Falls back to the raw type.
 */
function getElementLabel(type: ElementType): string {
  const labels: Record<ElementType, string> = {
    source: 'Source',
    sink: 'Sink',
    stream: 'Stream',
    table: 'Table',
    trigger: 'Trigger',
    window: 'Window',
    filter: 'Filter',
    projection: 'Projection',
    aggregation: 'Aggregation',
    groupBy: 'Group By',
    join: 'Join',
    pattern: 'Pattern',
    partition: 'Partition',
  };
  return labels[type] || type;
}

/**
 * Whether a node can take another incoming connection. False for an unknown node or type.
 */
export function canAcceptInput(nodeId: string, nodes: Node[], edges: Edge[]): boolean {
  const node = nodes.find((n) => n.id === nodeId);
  if (!node) return false;

  const rule = cardinalityRules[node.type as ElementType];
  if (!rule) return false;

  if (rule.maxInputs === null) return true;

  const currentInputs = edges.filter((e) => e.target === nodeId).length;
  return currentInputs < rule.maxInputs;
}

/**
 * Whether a node can take another outgoing connection. False for an unknown node or type.
 */
export function canHaveOutput(nodeId: string, nodes: Node[], edges: Edge[]): boolean {
  const node = nodes.find((n) => n.id === nodeId);
  if (!node) return false;

  const rule = cardinalityRules[node.type as ElementType];
  if (!rule) return false;

  if (rule.maxOutputs === null) return true;

  const currentOutputs = edges.filter((e) => e.source === nodeId).length;
  return currentOutputs < rule.maxOutputs;
}

/**
 * Input and output limits as text for the UI. No limit shows as ∞, an unknown type as ?.
 */
export function getConnectionInfo(type: ElementType): { inputs: string; outputs: string } {
  const rule = cardinalityRules[type];
  if (!rule) return { inputs: '?', outputs: '?' };

  return {
    inputs: rule.maxInputs === null ? '∞' : String(rule.maxInputs),
    outputs: rule.maxOutputs === null ? '∞' : String(rule.maxOutputs),
  };
}
