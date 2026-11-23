import { memo } from 'react';
import { Handle, Position, NodeProps } from '@xyflow/react';
import {
  Database,
  Table2,
  Timer,
  Clock,
  Filter,
  Columns,
  BarChart2,
  Group,
  GitMerge,
  Route,
  Layers,
  ArrowDownToLine,
  ArrowUpFromLine,
} from 'lucide-react';

// Source node. Brings external data in. It has only an output handle.
const SourceNode = memo(({ data, selected }: NodeProps) => {
  const props = data as { sourceName?: string; sourceType?: string; config?: Record<string, string> };

  const sourceLabels: Record<string, string> = {
    kafka: 'Kafka',
    http: 'HTTP',
    mqtt: 'MQTT',
    file: 'File',
    timer: 'Timer',
    websocket: 'WebSocket',
  };

  return (
    <div className={`element-node source ${selected ? 'selected ring-source' : ''}`}>
      <div className="element-node-header">
        <ArrowDownToLine className="w-4 h-4" />
        <span>{props.sourceName || 'Source'}</span>
      </div>
      <div className="element-node-body">
        <div className="flex items-center justify-center gap-2">
          <span className="px-2 py-0.5 bg-source/20 text-source rounded text-xs font-medium">
            {sourceLabels[props.sourceType || 'kafka'] || props.sourceType}
          </span>
        </div>
        {props.config && Object.keys(props.config).length > 0 && (
          <div className="mt-1 text-xs text-gray-500 text-center">
            {Object.keys(props.config).length} config params
          </div>
        )}
      </div>
      <Handle type="source" position={Position.Right} id="output" className="!bg-source" />
    </div>
  );
});

// Sink node. Sends data out. It has only an input handle.
const SinkNode = memo(({ data, selected }: NodeProps) => {
  const props = data as { sinkName?: string; sinkType?: string; config?: Record<string, string> };

  const sinkLabels: Record<string, string> = {
    kafka: 'Kafka',
    http: 'HTTP',
    mqtt: 'MQTT',
    file: 'File',
    log: 'Log',
    websocket: 'WebSocket',
  };

  return (
    <div className={`element-node sink ${selected ? 'selected ring-sink' : ''}`}>
      <Handle type="target" position={Position.Left} id="input" className="!bg-sink" />
      <div className="element-node-header">
        <ArrowUpFromLine className="w-4 h-4" />
        <span>{props.sinkName || 'Sink'}</span>
      </div>
      <div className="element-node-body">
        <div className="flex items-center justify-center gap-2">
          <span className="px-2 py-0.5 bg-sink/20 text-sink rounded text-xs font-medium">
            {sinkLabels[props.sinkType || 'log'] || props.sinkType}
          </span>
        </div>
        {props.config && Object.keys(props.config).length > 0 && (
          <div className="mt-1 text-xs text-gray-500 text-center">
            {Object.keys(props.config).length} config params
          </div>
        )}
      </div>
    </div>
  );
});

// Stream node. Carries events between elements. Shows the first 4 attributes.
const StreamNode = memo(({ data, selected }: NodeProps) => {
  const props = data as { streamName?: string; attributes?: { name: string; type: string }[] };

  return (
    <div className={`element-node stream ${selected ? 'selected ring-stream' : ''}`}>
      {/* Input handle. Fed by a source or by a query that inserts into this stream. */}
      <Handle type="target" position={Position.Left} id="input" className="!bg-stream" />
      <div className="element-node-header">
        <Database className="w-4 h-4" />
        <span>{props.streamName || 'Stream'}</span>
      </div>
      <div className="element-node-body">
        {props.attributes?.slice(0, 4).map((attr, i) => (
          <div key={i} className="element-node-attribute">
            <span className="element-node-attribute-name">{attr.name}</span>
            <span className="element-node-attribute-type">{attr.type}</span>
          </div>
        ))}
        {props.attributes && props.attributes.length > 4 && (
          <div className="text-gray-500 text-center">+{props.attributes.length - 4} more</div>
        )}
      </div>
      {/* Output handle. Feeds processing nodes or a sink. */}
      <Handle type="source" position={Position.Right} id="output" className="!bg-stream" />
    </div>
  );
});

// Table Node
const TableNode = memo(({ data, selected }: NodeProps) => {
  const props = data as { tableName?: string; attributes?: { name: string; type: string }[] };

  return (
    <div className={`element-node table ${selected ? 'selected ring-table' : ''}`}>
      <div className="element-node-header">
        <Table2 className="w-4 h-4" />
        <span>{props.tableName || 'Table'}</span>
      </div>
      <div className="element-node-body">
        {props.attributes?.slice(0, 4).map((attr, i) => (
          <div key={i} className="element-node-attribute">
            <span className="element-node-attribute-name">{attr.name}</span>
            <span className="element-node-attribute-type">{attr.type}</span>
          </div>
        ))}
      </div>
      <Handle type="source" position={Position.Right} id="output" className="!bg-table" />
    </div>
  );
});

// Trigger Node
const TriggerNode = memo(({ data, selected }: NodeProps) => {
  const props = data as { triggerId?: string; triggerType?: string; atEvery?: number; cronExpression?: string };

  const getLabel = () => {
    switch (props.triggerType) {
      case 'start':
        return 'AT START';
      case 'periodic':
        return `EVERY ${props.atEvery || 1000}ms`;
      case 'cron':
        return props.cronExpression || 'CRON';
      default:
        return 'Trigger';
    }
  };

  return (
    <div className={`element-node trigger ${selected ? 'selected ring-trigger' : ''}`}>
      <div className="element-node-header">
        <Timer className="w-4 h-4" />
        <span>{props.triggerId || 'Trigger'}</span>
      </div>
      <div className="element-node-body">
        <div className="text-center">{getLabel()}</div>
      </div>
      <Handle type="source" position={Position.Right} id="output" className="!bg-trigger" />
    </div>
  );
});

// Window Node
const WindowNode = memo(({ data, selected }: NodeProps) => {
  const props = data as { windowType?: string; parameters?: Record<string, unknown> };

  const getLabel = () => {
    const params = props.parameters || {};
    switch (props.windowType) {
      case 'length':
        return `length(${params.count || 10})`;
      case 'lengthBatch':
        return `lengthBatch(${params.count || 10})`;
      case 'time':
      case 'timeBatch':
      case 'tumbling':
        const dur = params.duration as { value: number; unit: string } | undefined;
        return `${props.windowType}(${dur?.value || 5} ${dur?.unit || 'SEC'})`;
      case 'sliding':
        const size = params.duration as { value: number; unit: string } | undefined;
        const slide = params.slideInterval as { value: number; unit: string } | undefined;
        return `sliding(${size?.value || 10}, ${slide?.value || 2})`;
      case 'session':
        const gap = params.gapDuration as { value: number; unit: string } | undefined;
        return `session(${gap?.value || 30} ${gap?.unit || 'SEC'})`;
      default:
        return props.windowType || 'window';
    }
  };

  return (
    <div className={`element-node window ${selected ? 'selected ring-window' : ''}`}>
      <Handle type="target" position={Position.Left} id="input" className="!bg-window" />
      <div className="element-node-header">
        <Clock className="w-4 h-4" />
        <span>Window</span>
      </div>
      <div className="element-node-body">
        <div className="text-center font-mono">{getLabel()}</div>
      </div>
      <Handle type="source" position={Position.Right} id="output" className="!bg-window" />
    </div>
  );
});

// Filter Node
const FilterNode = memo(({ data, selected }: NodeProps) => {
  const props = data as { condition?: Record<string, unknown> };

  const getConditionLabel = () => {
    const cond = props.condition;
    if (!cond) return 'No condition';
    const left = (cond.left as Record<string, unknown>)?.variableName as string || '?';
    const op = cond.operator as string || '=';
    const right = (cond.right as Record<string, unknown>)?.constantValue;
    return `${left} ${op} ${right ?? '?'}`;
  };

  return (
    <div className={`element-node filter ${selected ? 'selected ring-filter' : ''}`}>
      <Handle type="target" position={Position.Left} id="input" className="!bg-filter" />
      <div className="element-node-header">
        <Filter className="w-4 h-4" />
        <span>Filter</span>
      </div>
      <div className="element-node-body">
        <div className="text-center font-mono text-xs">{getConditionLabel()}</div>
      </div>
      <Handle type="source" position={Position.Right} id="output" className="!bg-filter" />
    </div>
  );
});

// Projection Node
const ProjectionNode = memo(({ data, selected }: NodeProps) => {
  const props = data as { selectList?: { alias?: string }[] };
  const count = props.selectList?.length || 0;

  return (
    <div className={`element-node projection ${selected ? 'selected ring-projection' : ''}`}>
      <Handle type="target" position={Position.Left} id="input" className="!bg-projection" />
      <div className="element-node-header">
        <Columns className="w-4 h-4" />
        <span>Projection</span>
      </div>
      <div className="element-node-body">
        <div className="text-center">{count > 0 ? `${count} columns` : 'SELECT *'}</div>
      </div>
      <Handle type="source" position={Position.Right} id="output" className="!bg-projection" />
    </div>
  );
});

// Aggregation Node
const AggregationNode = memo(({ data, selected }: NodeProps) => {
  const props = data as { aggregations?: { type: string; alias: string }[] };

  return (
    <div className={`element-node aggregation ${selected ? 'selected ring-aggregation' : ''}`}>
      <Handle type="target" position={Position.Left} id="input" className="!bg-aggregation" />
      <div className="element-node-header">
        <BarChart2 className="w-4 h-4" />
        <span>Aggregation</span>
      </div>
      <div className="element-node-body">
        {props.aggregations?.slice(0, 3).map((agg, i) => (
          <div key={i} className="element-node-attribute">
            <span className="element-node-attribute-name">{agg.type}</span>
            <span className="element-node-attribute-type">{agg.alias}</span>
          </div>
        ))}
        {(!props.aggregations || props.aggregations.length === 0) && (
          <div className="text-center text-gray-500">No aggregations</div>
        )}
      </div>
      <Handle type="source" position={Position.Right} id="output" className="!bg-aggregation" />
    </div>
  );
});

// Group By Node
const GroupByNode = memo(({ data, selected }: NodeProps) => {
  const props = data as { groupByAttributes?: string[] };

  return (
    <div className={`element-node aggregation ${selected ? 'selected ring-aggregation' : ''}`}>
      <Handle type="target" position={Position.Left} id="input" className="!bg-aggregation" />
      <div className="element-node-header">
        <Group className="w-4 h-4" />
        <span>Group By</span>
      </div>
      <div className="element-node-body">
        <div className="text-center font-mono text-xs">
          {props.groupByAttributes?.join(', ') || 'No columns'}
        </div>
      </div>
      <Handle type="source" position={Position.Right} id="output" className="!bg-aggregation" />
    </div>
  );
});

// Join Node
const JoinNode = memo(({ data, selected }: NodeProps) => {
  const props = data as { joinType?: string };

  const typeLabel = {
    inner: 'INNER JOIN',
    left_outer: 'LEFT JOIN',
    right_outer: 'RIGHT JOIN',
    full_outer: 'FULL JOIN',
  }[props.joinType || 'inner'] || 'JOIN';

  return (
    <div className={`element-node join ${selected ? 'selected ring-join' : ''}`}>
      <Handle type="target" position={Position.Left} id="left" style={{ top: '30%' }} className="!bg-join" />
      <Handle type="target" position={Position.Left} id="right" style={{ top: '70%' }} className="!bg-join" />
      <div className="element-node-header">
        <GitMerge className="w-4 h-4" />
        <span>Join</span>
      </div>
      <div className="element-node-body">
        <div className="text-center">{typeLabel}</div>
      </div>
      <Handle type="source" position={Position.Right} id="output" className="!bg-join" />
    </div>
  );
});

// Pattern Node
const PatternNode = memo(({ data, selected }: NodeProps) => {
  const props = data as { mode?: string; withinConstraint?: { value: number; unit?: string } };

  return (
    <div className={`element-node pattern ${selected ? 'selected ring-pattern' : ''}`}>
      <Handle type="target" position={Position.Left} id="input-1" style={{ top: '30%' }} className="!bg-pattern" />
      <Handle type="target" position={Position.Left} id="input-2" style={{ top: '70%' }} className="!bg-pattern" />
      <div className="element-node-header">
        <Route className="w-4 h-4" />
        <span>{props.mode?.toUpperCase() || 'PATTERN'}</span>
      </div>
      <div className="element-node-body">
        {props.withinConstraint && (
          <div className="text-center text-xs">
            WITHIN {props.withinConstraint.value} {props.withinConstraint.unit || ''}
          </div>
        )}
      </div>
      <Handle type="source" position={Position.Right} id="output" className="!bg-pattern" />
    </div>
  );
});

// Partition Node
const PartitionNode = memo(({ data, selected }: NodeProps) => {
  const props = data as { partitionBy?: { attribute: string }[] };

  return (
    <div className={`element-node partition ${selected ? 'selected ring-partition' : ''}`}>
      <Handle type="target" position={Position.Left} id="input" className="!bg-partition" />
      <div className="element-node-header">
        <Layers className="w-4 h-4" />
        <span>Partition</span>
      </div>
      <div className="element-node-body">
        <div className="text-center font-mono text-xs">
          {props.partitionBy?.map((p) => p.attribute).join(', ') || 'No partition key'}
        </div>
      </div>
      <Handle type="source" position={Position.Right} id="output" className="!bg-partition" />
    </div>
  );
});

// Maps each element type to the component React Flow renders for it
export const nodeTypes = {
  source: SourceNode,
  stream: StreamNode,
  table: TableNode,
  trigger: TriggerNode,
  window: WindowNode,
  filter: FilterNode,
  projection: ProjectionNode,
  aggregation: AggregationNode,
  groupBy: GroupByNode,
  join: JoinNode,
  pattern: PatternNode,
  partition: PartitionNode,
  sink: SinkNode,
};
