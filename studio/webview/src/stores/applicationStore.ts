import { create } from 'zustand';
import { subscribeWithSelector } from 'zustand/middleware';
import { Node, Edge, NodeChange, EdgeChange, applyNodeChanges, applyEdgeChanges } from '@xyflow/react';
import type { VisualApplication, VisualElement, Connection, StudioConfig, ElementType, ElementProperties } from '../types';
import { vscode } from '../utils/vscode';
import { parseSQL, validateSQL } from '../utils/sqlParser';
import { getSourceTypes, getSinkTypes, getSourceSchema, getSinkSchema, getWindowTypes } from '../schemas';

// Returns a wrapper that runs fn only after delay ms with no new call. Used to batch saves.
function debounce<T extends (...args: unknown[]) => void>(fn: T, delay: number): T {
  let timeoutId: ReturnType<typeof setTimeout> | null = null;
  return ((...args: unknown[]) => {
    if (timeoutId) clearTimeout(timeoutId);
    timeoutId = setTimeout(() => fn(...args), delay);
  }) as T;
}

// An attribute that reaches an element from upstream
export interface UpstreamAttribute {
  name: string;
  type: string;
  streamId?: string;
  source: string; // id of the element that defined this attribute
}

// Canvas state saved for undo and redo
interface HistorySnapshot {
  nodes: Node[];
  edges: Edge[];
}

interface ApplicationState {
  // Application data
  application: {
    elements: VisualElement[];
    connections: Connection[];
  };
  name: string;
  isDirty: boolean;

  // React Flow state
  nodes: Node[];
  edges: Edge[];

  // Selection state
  selectedElementIds: string[];
  selectedConnectionId: string | null;

  // UI state
  viewMode: 'visual' | 'sql' | 'split';
  generatedSQL: string;

  // Configuration
  config: StudioConfig;

  // Undo and redo history. past holds states to undo to, future holds states to redo to.
  past: HistorySnapshot[];
  future: HistorySnapshot[];

  // Actions
  loadApplication: (data: VisualApplication) => void;
  setNodes: (nodes: Node[]) => void;
  setEdges: (edges: Edge[]) => void;
  onNodesChange: (changes: NodeChange[]) => void;
  onEdgesChange: (changes: EdgeChange[]) => void;

  addElement: (type: ElementType, position: { x: number; y: number }, properties?: Record<string, unknown>) => void;
  removeElement: (id: string) => void;
  updateElement: (id: string, changes: Partial<VisualElement> | { data: Record<string, unknown> }) => void;

  addConnection: (connection: Connection) => void;
  removeConnection: (id: string) => void;

  selectElement: (id: string | null, multi?: boolean) => void;
  clearSelection: () => void;

  setViewMode: (mode: 'visual' | 'sql' | 'split') => void;
  regenerateSQL: () => void;

  // SQL sync
  updateSQL: (sql: string) => void;
  importSQL: (sql: string) => { success: boolean; errors: string[] };
  getSQLValidation: (sql: string) => { valid: boolean; errors: string[] };

  save: () => void;
  debouncedSave: () => void;
  setConfig: (config: Partial<StudioConfig>) => void;

  // Undo and redo
  pushHistory: () => void;
  undo: () => void;
  redo: () => void;
  canUndo: () => boolean;
  canRedo: () => boolean;

  // Schema detection
  getUpstreamSchema: (elementId: string) => UpstreamAttribute[];
  getAllStreams: () => { id: string; name: string; attributes: UpstreamAttribute[] }[];
}

const defaultConfig: StudioConfig = {
  gridSize: 20,
  snapToGrid: true,
  autoSave: true,
  engineHost: 'localhost',
  enginePort: 9090,
};

// The store's save function. Set on the first debouncedSave call, after the store exists.
let saveToVSCode: (() => void) | null = null;

// Saves 500 ms after the last change, so a burst of edits causes one save
const debouncedSave = debounce(() => {
  if (saveToVSCode) saveToVSCode();
}, 500);

export const useApplicationStore = create<ApplicationState>()(
  subscribeWithSelector((set, get) => ({
    // Initial state
    application: {
      elements: [],
      connections: [],
    },
    name: 'Untitled Project',
    isDirty: false,
    nodes: [],
    edges: [],
    selectedElementIds: [],
    selectedConnectionId: null,
    viewMode: 'visual',
    generatedSQL: '',
    config: defaultConfig,
    past: [],
    future: [],

    // Replace the whole canvas with a loaded application and clear undo history
    loadApplication: (data) => {
      const nodes: Node[] = data.application.elements.map((el) => ({
        id: el.id,
        type: el.type,
        position: el.position,
        data: el.properties as unknown as Record<string, unknown>,
      }));

      const edges: Edge[] = data.application.connections.map((conn) => ({
        id: conn.id,
        source: conn.sourceElementId,
        sourceHandle: conn.sourcePortId,
        target: conn.targetElementId,
        targetHandle: conn.targetPortId,
        animated: true,
      }));

      set({
        application: data.application,
        name: data.name,
        nodes,
        edges,
        isDirty: false,
        past: [],
        future: [],
      });

      get().regenerateSQL();
    },

    // React Flow node and edge state. These keep application in step with the canvas.
    setNodes: (nodes) => {
      const elements: VisualElement[] = nodes.map((node) => ({
        id: node.id,
        type: node.type as ElementType,
        position: node.position as { x: number; y: number },
        properties: node.data as unknown as ElementProperties,
      }));

      set((state) => ({
        nodes,
        application: {
          ...state.application,
          elements,
        },
        isDirty: true,
      }));

      get().regenerateSQL();
      if (get().config.autoSave) {
        get().debouncedSave();
      }
    },

    setEdges: (edges) => {
      const connections = edges.map((edge) => ({
        id: edge.id,
        sourceElementId: edge.source,
        sourcePortId: edge.sourceHandle || 'output',
        targetElementId: edge.target,
        targetPortId: edge.targetHandle || 'input',
      }));

      set((state) => ({
        edges,
        application: {
          ...state.application,
          connections,
        },
        isDirty: true,
      }));

      get().regenerateSQL();
      if (get().config.autoSave) {
        get().debouncedSave();
      }
    },

    onNodesChange: (changes) => {
      set((state) => ({
        nodes: applyNodeChanges(changes, state.nodes),
      }));
    },

    onEdgesChange: (changes) => {
      set((state) => ({
        edges: applyEdgeChanges(changes, state.edges),
      }));
    },

    // Element management
    addElement: (type, position, properties = {}) => {
      // Save the current state so this change can be undone
      get().pushHistory();

      const id = `${type}-${Date.now()}`;
      const data = { ...getDefaultProperties(type), ...properties };
      const newNode: Node = {
        id,
        type,
        position,
        data,
      };
      const newElement: VisualElement = {
        id,
        type,
        position,
        properties: data as unknown as ElementProperties,
      };

      set((state) => ({
        nodes: [...state.nodes, newNode],
        application: {
          ...state.application,
          elements: [...state.application.elements, newElement],
        },
        isDirty: true,
      }));

      get().regenerateSQL();
      if (get().config.autoSave) {
        get().debouncedSave();
      }
    },

    removeElement: (id) => {
      // Save the current state so this change can be undone
      get().pushHistory();

      set((state) => ({
        nodes: state.nodes.filter((n) => n.id !== id),
        edges: state.edges.filter((e) => e.source !== id && e.target !== id),
        application: {
          elements: state.application.elements.filter((el) => el.id !== id),
          connections: state.application.connections.filter(
            (c) => c.sourceElementId !== id && c.targetElementId !== id
          ),
        },
        selectedElementIds: state.selectedElementIds.filter((i) => i !== id),
        isDirty: true,
      }));

      get().regenerateSQL();
      if (get().config.autoSave) {
        get().debouncedSave();
      }
    },

    updateElement: (id, changes) => {
      set((state) => {
        const updatedNodes = state.nodes.map((node) => {
          if (node.id !== id) return node;

          // A data change from the properties panel is merged into the existing data.
          // updateElement does not push undo history.
          const nodeChanges = changes as { data?: Record<string, unknown> };
          if (nodeChanges.data) {
            return {
              ...node,
              data: { ...node.data, ...nodeChanges.data },
            };
          }

          // Any other change is spread onto the node as is
          return { ...node, ...changes };
        });

        // Keep application.elements in step with the updated node
        const updatedElements = state.application.elements.map((el) => {
          if (el.id !== id) return el;
          const node = updatedNodes.find((n) => n.id === id);
          if (!node) return el;
          return {
            ...el,
            position: node.position as { x: number; y: number },
            properties: node.data as unknown as ElementProperties,
          };
        });

        return {
          nodes: updatedNodes,
          application: {
            ...state.application,
            elements: updatedElements,
          },
          isDirty: true,
        };
      });

      get().regenerateSQL();
      if (get().config.autoSave) {
        get().debouncedSave();
      }
    },

    // Connection management. These update edges only, not application.connections,
    // and do not trigger an auto save.
    addConnection: (connection) => {
      // Save the current state so this change can be undone
      get().pushHistory();

      const edge: Edge = {
        id: connection.id,
        source: connection.sourceElementId,
        sourceHandle: connection.sourcePortId,
        target: connection.targetElementId,
        targetHandle: connection.targetPortId,
        animated: true,
      };

      set((state) => ({
        edges: [...state.edges, edge],
        isDirty: true,
      }));

      get().regenerateSQL();
    },

    removeConnection: (id) => {
      // Save the current state so this change can be undone
      get().pushHistory();

      set((state) => ({
        edges: state.edges.filter((e) => e.id !== id),
        selectedConnectionId: state.selectedConnectionId === id ? null : state.selectedConnectionId,
        isDirty: true,
      }));

      get().regenerateSQL();
    },

    // Selection
    selectElement: (id, multi = false) => {
      set((state) => {
        if (id === null) {
          return { selectedElementIds: [] };
        }
        if (multi) {
          const isSelected = state.selectedElementIds.includes(id);
          return {
            selectedElementIds: isSelected
              ? state.selectedElementIds.filter((i) => i !== id)
              : [...state.selectedElementIds, id],
          };
        }
        return { selectedElementIds: [id] };
      });
    },

    clearSelection: () => {
      set({ selectedElementIds: [], selectedConnectionId: null });
    },

    // View mode
    setViewMode: (mode) => {
      set({ viewMode: mode });
    },

    // Rebuild the SQL from application. This overwrites any manual SQL edits.
    regenerateSQL: () => {
      const { application } = get();
      const sql = generateSQL(application);
      set({ generatedSQL: sql });
    },

    // Store SQL typed by the user. The canvas is not changed until importSQL runs.
    updateSQL: (sql: string) => {
      set({ generatedSQL: sql });
    },

    // Parse SQL and replace the canvas with the result. Fails, leaving the canvas
    // as it was, when parsing reports errors or finds no elements.
    importSQL: (sql: string) => {
      const { elements, errors } = parseSQL(sql);

      if (errors.length > 0) {
        return { success: false, errors };
      }

      if (elements.length === 0) {
        return { success: false, errors: ['No valid elements found in SQL'] };
      }

      const nodes: Node[] = elements.map((el) => ({
        id: el.id,
        type: el.type,
        position: el.position,
        data: el.properties as unknown as Record<string, unknown>,
      }));

      // Connect each element to the next one in parse order. This is a guess at the
      // flow, not read from the SQL.
      const edges: Edge[] = [];
      for (let i = 0; i < elements.length - 1; i++) {
        const current = elements[i];
        const next = elements[i + 1];

        // Two definitions in a row (stream, table or trigger) are not connected
        if (['stream', 'table', 'trigger'].includes(current.type) &&
            ['stream', 'table', 'trigger'].includes(next.type)) {
          continue;
        }

        edges.push({
          id: `edge-${Date.now()}-${i}`,
          source: current.id,
          target: next.id,
          sourceHandle: 'output',
          targetHandle: 'input',
          animated: true,
        });
      }

      set({
        nodes,
        edges,
        application: {
          elements,
          connections: edges.map((e) => ({
            id: e.id,
            sourceElementId: e.source,
            sourcePortId: e.sourceHandle || 'output',
            targetElementId: e.target,
            targetPortId: e.targetHandle || 'input',
          })),
        },
        generatedSQL: sql,
        isDirty: true,
      });

      if (get().config.autoSave) {
        get().debouncedSave();
      }

      return { success: true, errors: [] };
    },

    getSQLValidation: (sql: string) => {
      return validateSQL(sql);
    },

    // Send the full project to the extension, which writes it to disk.
    // Built from nodes and edges, not from application.
    save: () => {
      const state = get();
      const elements: VisualElement[] = state.nodes.map((node) => ({
        id: node.id,
        type: node.type as ElementType,
        position: node.position as { x: number; y: number },
        properties: node.data as unknown as ElementProperties,
      }));

      const data: VisualApplication = {
        $schema: 'https://eventflux.io/schemas/studio/v1.json',
        version: '1.0',
        name: state.name,
        application: {
          elements,
          connections: state.edges.map((edge) => ({
            id: edge.id,
            sourceElementId: edge.source,
            sourcePortId: edge.sourceHandle || 'output',
            targetElementId: edge.target,
            targetPortId: edge.targetHandle || 'input',
          })),
        },
        layout: {
          zoom: 1,
          pan: { x: 0, y: 0 },
          gridSize: state.config.gridSize,
          snapToGrid: state.config.snapToGrid,
        },
        metadata: {
          created: new Date().toISOString(),
          modified: new Date().toISOString(),
        },
      };

      vscode.postMessage({ type: 'update', content: data });
      set({ isDirty: false });
    },

    // Save used by auto save. Runs 500 ms after the last call.
    debouncedSave: () => {
      if (!saveToVSCode) {
        saveToVSCode = () => get().save();
      }
      debouncedSave();
    },

    // Configuration
    setConfig: (config) => {
      set((state) => ({
        config: { ...state.config, ...config },
      }));
    },

    // Attributes available to an element, gathered from the elements upstream of it
    getUpstreamSchema: (elementId: string): UpstreamAttribute[] => {
      const { nodes, edges } = get();
      const attributes: UpstreamAttribute[] = [];
      const visited = new Set<string>();

      // Walk incoming edges back through the graph. visited guards against cycles.
      const findUpstreamElements = (nodeId: string): Node[] => {
        if (visited.has(nodeId)) return [];
        visited.add(nodeId);

        const result: Node[] = [];
        const incomingEdges = edges.filter((e) => e.target === nodeId);

        for (const edge of incomingEdges) {
          const sourceNode = nodes.find((n) => n.id === edge.source);
          if (sourceNode) {
            result.push(sourceNode);
            result.push(...findUpstreamElements(sourceNode.id));
          }
        }

        return result;
      };

      const upstreamNodes = findUpstreamElements(elementId);

      // Streams and tables add their attributes. Projections add aliases and
      // aggregations add their outputs. Other node types add nothing.
      for (const node of upstreamNodes) {
        const data = node.data as Record<string, unknown>;

        switch (node.type) {
          case 'stream': {
            const streamName = (data.streamName as string) || '';
            const attrs = (data.attributes as { name: string; type: string }[]) || [];
            for (const attr of attrs) {
              attributes.push({
                name: attr.name,
                type: attr.type,
                streamId: streamName,
                source: node.id,
              });
            }
            break;
          }
          case 'table': {
            const tableName = (data.tableName as string) || '';
            const attrs = (data.attributes as { name: string; type: string }[]) || [];
            for (const attr of attrs) {
              attributes.push({
                name: attr.name,
                type: attr.type,
                streamId: tableName,
                source: node.id,
              });
            }
            break;
          }
          case 'projection': {
            // Only items with an alias add a new attribute
            const selectList = (data.selectList as { expression: Record<string, unknown>; alias?: string }[]) || [];
            for (const item of selectList) {
              const alias = item.alias;
              const expr = item.expression;
              if (alias) {
                // A plain variable keeps the type of the attribute it names, if
                // that attribute was already found. Anything else is STRING.
                let attrType = 'STRING';
                if (expr?.type === 'variable' && expr?.variableName) {
                  const origAttr = attributes.find((a) => a.name === expr.variableName);
                  if (origAttr) attrType = origAttr.type;
                }
                attributes.push({
                  name: alias,
                  type: attrType,
                  source: node.id,
                });
              }
            }
            break;
          }
          case 'aggregation': {
            // Each aggregation adds its alias. COUNT and DISTINCTCOUNT give LONG,
            // FIRST and LAST give STRING, everything else DOUBLE.
            const aggs = (data.aggregations as { type: string; alias: string }[]) || [];
            for (const agg of aggs) {
              let resultType = 'DOUBLE';
              if (agg.type === 'COUNT' || agg.type === 'DISTINCTCOUNT') {
                resultType = 'LONG';
              } else if (agg.type === 'FIRST' || agg.type === 'LAST') {
                // The real type is the input type, which is not looked up here
                resultType = 'STRING';
              }
              attributes.push({
                name: agg.alias,
                type: resultType,
                source: node.id,
              });
            }
            break;
          }
        }
      }

      return attributes;
    },

    // Every stream and table on the canvas, connected or not
    getAllStreams: () => {
      const { nodes } = get();
      const result: { id: string; name: string; attributes: UpstreamAttribute[] }[] = [];

      for (const node of nodes) {
        const data = node.data as Record<string, unknown>;

        if (node.type === 'stream') {
          const streamName = (data.streamName as string) || '';
          const attrs = (data.attributes as { name: string; type: string }[]) || [];
          result.push({
            id: node.id,
            name: streamName,
            attributes: attrs.map((a) => ({
              name: a.name,
              type: a.type,
              streamId: streamName,
              source: node.id,
            })),
          });
        } else if (node.type === 'table') {
          const tableName = (data.tableName as string) || '';
          const attrs = (data.attributes as { name: string; type: string }[]) || [];
          result.push({
            id: node.id,
            name: tableName,
            attributes: attrs.map((a) => ({
              name: a.name,
              type: a.type,
              streamId: tableName,
              source: node.id,
            })),
          });
        }
      }

      return result;
    },

    // Undo and redo with past and future stacks. Snapshots are deep copies made
    // through JSON, and hold nodes and edges only.
    pushHistory: () => {
      const { nodes, edges, past } = get();
      const snapshot: HistorySnapshot = {
        nodes: JSON.parse(JSON.stringify(nodes)),
        edges: JSON.parse(JSON.stringify(edges)),
      };
      // A new change makes the redo stack meaningless, so clear it
      const newPast = [...past, snapshot];
      // Keep at most 50 states. The oldest is dropped.
      if (newPast.length > 50) {
        newPast.shift();
      }
      set({ past: newPast, future: [] });
    },

    undo: () => {
      const { nodes, edges, past, future } = get();
      if (past.length === 0) return;

      // Push the current state onto future so redo can bring it back
      const currentSnapshot: HistorySnapshot = {
        nodes: JSON.parse(JSON.stringify(nodes)),
        edges: JSON.parse(JSON.stringify(edges)),
      };

      const newPast = [...past];
      const previousState = newPast.pop()!;

      set({
        nodes: JSON.parse(JSON.stringify(previousState.nodes)),
        edges: JSON.parse(JSON.stringify(previousState.edges)),
        past: newPast,
        future: [...future, currentSnapshot],
        isDirty: true,
      });

      // Regenerate on the next tick. regenerateSQL reads application, which undo does not
      // restore, so the SQL can still show the state before the undo.
      setTimeout(() => get().regenerateSQL(), 0);
    },

    redo: () => {
      const { nodes, edges, past, future } = get();
      if (future.length === 0) return;

      // Push the current state onto past so undo can bring it back
      const currentSnapshot: HistorySnapshot = {
        nodes: JSON.parse(JSON.stringify(nodes)),
        edges: JSON.parse(JSON.stringify(edges)),
      };

      const newFuture = [...future];
      const nextState = newFuture.pop()!;

      set({
        nodes: JSON.parse(JSON.stringify(nextState.nodes)),
        edges: JSON.parse(JSON.stringify(nextState.edges)),
        past: [...past, currentSnapshot],
        future: newFuture,
        isDirty: true,
      });

      // Regenerate on the next tick. regenerateSQL reads application, which redo does not
      // restore, so the SQL can still show the state before the redo.
      setTimeout(() => get().regenerateSQL(), 0);
    },

    canUndo: () => get().past.length > 0,
    canRedo: () => get().future.length > 0,
  }))
);

// Helpers
function getDefaultProperties(type: ElementType): Record<string, unknown> {
  switch (type) {
    case 'source': {
      // Prefer websocket, then the first schema source type, then 'timer'
      const sourceTypes = getSourceTypes();
      const defaultSourceType = sourceTypes.includes('websocket') ? 'websocket' : sourceTypes[0] || 'timer';
      const schema = getSourceSchema(defaultSourceType);
      // Start with an empty entry for each required parameter
      const config: Record<string, string> = {};
      if (schema?.requiredParameters) {
        for (const param of schema.requiredParameters) {
          config[param] = '';
        }
      }
      return {
        sourceName: 'NewSource',
        sourceType: defaultSourceType,
        config,
      };
    }
    case 'sink': {
      // First sink type in the schema, or 'log' when there is none
      const sinkTypes = getSinkTypes();
      const defaultSinkType = sinkTypes[0] || 'log';
      const schema = getSinkSchema(defaultSinkType);
      // Start with an empty entry for each required parameter
      const config: Record<string, string> = {};
      if (schema?.requiredParameters) {
        for (const param of schema.requiredParameters) {
          config[param] = '';
        }
      }
      return {
        sinkName: 'NewSink',
        sinkType: defaultSinkType,
        config,
      };
    }
    case 'stream':
      return {
        streamName: 'NewStream',
        attributes: [
          { name: 'id', type: 'INT' },
          { name: 'value', type: 'DOUBLE' },
        ],
      };
    case 'table':
      return {
        tableName: 'NewTable',
        attributes: [
          { name: 'key', type: 'STRING' },
          { name: 'value', type: 'STRING' },
        ],
      };
    case 'trigger':
      return {
        triggerId: 'NewTrigger',
        triggerType: 'periodic',
        atEvery: 1000,
      };
    case 'window': {
      // Prefer length, then the first schema window type
      const windowTypes = getWindowTypes();
      const defaultWindowType = windowTypes.includes('length') ? 'length' : windowTypes[0] || 'length';
      return {
        windowType: defaultWindowType,
        parameters: { count: 10 },
      };
    }
    case 'filter':
      return {
        condition: null,
      };
    case 'projection':
      return {
        selectList: [],
      };
    case 'aggregation':
      return {
        aggregations: [],
      };
    case 'groupBy':
      return {
        groupByAttributes: [],
      };
    case 'join':
      return {
        joinType: 'inner',
        onCondition: null,
      };
    case 'pattern':
      return {
        mode: 'pattern',
        patternExpression: { type: 'stream', streamName: '' },
      };
    case 'partition':
      return {
        partitionBy: [],
      };
    default:
      return {};
  }
}

function generateSQL(application: { elements: VisualElement[]; connections: Connection[] }): string {
  const lines: string[] = [];
  const { elements, connections } = application;

  const streams = elements.filter((el) => el.type === 'stream');
  for (const stream of streams) {
    const props = stream.properties as { streamName: string; attributes: { name: string; type: string }[] };
    const attrs = props.attributes?.map((a) => `${a.name} ${a.type}`).join(', ') || '';
    lines.push(`CREATE STREAM ${props.streamName} (${attrs});`);
  }

  const tables = elements.filter((el) => el.type === 'table');
  for (const table of tables) {
    const props = table.properties as { tableName: string; attributes: { name: string; type: string }[] };
    const attrs = props.attributes?.map((a) => `${a.name} ${a.type}`).join(', ') || '';
    lines.push(`CREATE TABLE ${props.tableName} (${attrs});`);
  }

  const triggers = elements.filter((el) => el.type === 'trigger');
  for (const trigger of triggers) {
    const props = trigger.properties as { triggerId: string; triggerType: string; atEvery?: number; cronExpression?: string };
    if (props.triggerType === 'start') {
      lines.push(`CREATE TRIGGER ${props.triggerId} AT START;`);
    } else if (props.triggerType === 'periodic') {
      lines.push(`CREATE TRIGGER ${props.triggerId} AT EVERY ${props.atEvery} MILLISECONDS;`);
    } else if (props.triggerType === 'cron') {
      lines.push(`CREATE TRIGGER ${props.triggerId} AT CRON '${props.cronExpression}';`);
    }
  }

  if (lines.length > 0) {
    lines.push('');
  }

  // One INSERT INTO query per connection from a processing element into a stream.
  // Connections from sources do not produce queries.
  const processingTypes = ['window', 'filter', 'projection', 'aggregation', 'groupBy', 'join', 'pattern', 'partition'];

  let queryCount = 0;
  for (const conn of connections) {
    const sourceEl = elements.find((el) => el.id === conn.sourceElementId);
    const targetEl = elements.find((el) => el.id === conn.targetElementId);

    if (sourceEl && targetEl && processingTypes.includes(sourceEl.type) && targetEl.type === 'stream') {
      const targetStreamName = (targetEl.properties as { streamName: string }).streamName;
      const query = buildQueryFromProcessingToStream(sourceEl, targetStreamName, elements, connections);
      if (query) {
        if (queryCount > 0) {
          lines.push('');
        }
        lines.push(query);
        queryCount++;
      }
    }
  }

  // A stream connected straight to a sink produces no query yet. Only connections
  // from a processing element into a stream produce INSERT INTO queries.

  return lines.join('\n');
}

// Builds an INSERT INTO query for a processing element that feeds targetStreamName.
// Returns null when there is no pipeline to build from.
function buildQueryFromProcessingToStream(
  processingElement: VisualElement,
  targetStreamName: string,
  elements: VisualElement[],
  connections: Connection[]
): string | null {
  const pipeline = traceBackPipeline(processingElement.id, elements, connections);
  if (pipeline.length === 0) return null;

  let fromClause = '';
  let windowClause = '';
  let whereClause = '';
  let selectClause = 'SELECT *';
  let groupByClause = '';
  let havingClause = '';

  for (const el of pipeline) {
    switch (el.type) {
      case 'stream': {
        const props = el.properties as { streamName: string };
        fromClause = props.streamName;
        break;
      }
      case 'window': {
        const props = el.properties as { windowType: string; parameters: Record<string, unknown> };
        windowClause = generateWindowClause(props);
        break;
      }
      case 'filter': {
        const props = el.properties as { condition: unknown };
        if (props.condition) {
          whereClause = `WHERE ${expressionToSQL(props.condition)}`;
        }
        break;
      }
      case 'projection': {
        const props = el.properties as { selectList: { expression: unknown; alias?: string }[] };
        if (props.selectList && props.selectList.length > 0) {
          selectClause = 'SELECT ' + props.selectList
            .map((item) => {
              const expr = expressionToSQL(item.expression);
              return item.alias ? `${expr} AS ${item.alias}` : expr;
            })
            .join(', ');
        }
        break;
      }
      case 'aggregation': {
        const props = el.properties as { aggregations: { type: string; expression: unknown; alias: string }[] };
        if (props.aggregations && props.aggregations.length > 0) {
          const aggParts = props.aggregations.map((agg) => {
            const expr = expressionToSQL(agg.expression);
            return `${agg.type}(${expr}) AS ${agg.alias}`;
          });
          selectClause = 'SELECT ' + aggParts.join(', ');
        }
        break;
      }
      case 'groupBy': {
        const props = el.properties as { groupByAttributes: string[]; havingCondition?: unknown };
        if (props.groupByAttributes && props.groupByAttributes.length > 0) {
          groupByClause = `GROUP BY ${props.groupByAttributes.join(', ')}`;
        }
        if (props.havingCondition) {
          havingClause = `HAVING ${expressionToSQL(props.havingCondition)}`;
        }
        break;
      }
    }
  }

  if (!fromClause) return null;

  const parts = [
    `INSERT INTO ${targetStreamName}`,
    selectClause,
    `FROM ${fromClause}${windowClause ? ' ' + windowClause : ''}`,
  ];

  if (whereClause) parts.push(whereClause);
  if (groupByClause) parts.push(groupByClause);
  if (havingClause) parts.push(havingClause);

  return parts.join('\n') + ';';
}

function traceBackPipeline(
  elementId: string,
  elements: VisualElement[],
  connections: Connection[]
): VisualElement[] {
  const result: VisualElement[] = [];
  let currentId: string | null = elementId;

  while (currentId) {
    const element = elements.find((el) => el.id === currentId);
    if (!element) break;

    result.unshift(element);

    // Follow only the first incoming connection. There is no cycle check.
    const inputConn = connections.find((c) => c.targetElementId === currentId);
    currentId = inputConn ? inputConn.sourceElementId : null;
  }

  return result;
}

function generateWindowClause(props: { windowType: string; parameters: Record<string, unknown> }): string {
  const { windowType, parameters } = props;

  switch (windowType) {
    case 'length':
      return `WINDOW('length', ${parameters.count})`;
    case 'lengthBatch':
      return `WINDOW('lengthBatch', ${parameters.count})`;
    case 'time':
      return `WINDOW('time', ${formatDuration(parameters.duration as { value: number; unit: string })})`;
    case 'timeBatch':
      return `WINDOW('timeBatch', ${formatDuration(parameters.duration as { value: number; unit: string })})`;
    case 'tumbling':
      return `WINDOW('tumbling', ${formatDuration(parameters.duration as { value: number; unit: string })})`;
    case 'sliding':
      return `WINDOW('sliding', ${formatDuration(parameters.duration as { value: number; unit: string })}, ${formatDuration(parameters.slideInterval as { value: number; unit: string })})`;
    case 'session':
      return `WINDOW('session', ${formatDuration(parameters.gapDuration as { value: number; unit: string })})`;
    case 'externalTime':
      return `WINDOW('externalTime', ${parameters.timestampAttribute}, ${formatDuration(parameters.duration as { value: number; unit: string })})`;
    case 'externalTimeBatch':
      return `WINDOW('externalTimeBatch', ${parameters.timestampAttribute}, ${formatDuration(parameters.duration as { value: number; unit: string })})`;
    case 'sort':
      return `WINDOW('sort', ${parameters.count}, ${parameters.sortAttribute})`;
    default:
      return '';
  }
}

function formatDuration(duration: { value: number; unit: string } | undefined): string {
  if (!duration) return '0 SECONDS';
  return `${duration.value} ${duration.unit}`;
}

function expressionToSQL(expr: unknown): string {
  if (!expr) return '*';
  if (typeof expr !== 'object') return String(expr);

  const e = expr as Record<string, unknown>;
  switch (e.type) {
    case 'constant':
      if (e.constantType === 'string') return `'${e.constantValue}'`;
      if (e.constantType === 'null') return 'NULL';
      return String(e.constantValue);

    case 'variable':
      return e.streamId ? `${e.streamId}.${e.variableName}` : String(e.variableName);

    case 'function':
      const params = (e.parameters as unknown[] || []).map((p) => expressionToSQL(p)).join(', ');
      return `${String(e.functionName).toUpperCase()}(${params})`;

    case 'compare':
      return `${expressionToSQL(e.left)} ${e.operator} ${expressionToSQL(e.right)}`;

    case 'and':
      return `(${expressionToSQL(e.left)} AND ${expressionToSQL(e.right)})`;

    case 'or':
      return `(${expressionToSQL(e.left)} OR ${expressionToSQL(e.right)})`;

    case 'not':
      return `NOT ${expressionToSQL(e.operand)}`;

    case 'add':
      return `(${expressionToSQL(e.left)} + ${expressionToSQL(e.right)})`;

    case 'subtract':
      return `(${expressionToSQL(e.left)} - ${expressionToSQL(e.right)})`;

    case 'multiply':
      return `(${expressionToSQL(e.left)} * ${expressionToSQL(e.right)})`;

    case 'divide':
      return `(${expressionToSQL(e.left)} / ${expressionToSQL(e.right)})`;

    default:
      return '*';
  }
}
