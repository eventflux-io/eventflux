// Visual Application Types

export interface VisualApplication {
  $schema?: string;
  version: string;
  name: string;
  application: {
    elements: VisualElement[];
    connections: Connection[];
  };
  layout: LayoutSettings;
  metadata: ApplicationMetadata;
  importedSQL?: string;
}

export interface VisualElement {
  id: string;
  type: ElementType;
  position: Position;
  size?: Size;
  properties: ElementProperties;
}

export interface Connection {
  id: string;
  sourceElementId: string;
  sourcePortId: string;
  targetElementId: string;
  targetPortId: string;
}

export interface Position {
  x: number;
  y: number;
}

export interface Size {
  width: number;
  height: number;
}

export interface LayoutSettings {
  zoom: number;
  pan: Position;
  gridSize: number;
  snapToGrid: boolean;
}

export interface ApplicationMetadata {
  created: string;
  modified: string;
  author?: string;
  importedFrom?: string;
}

// Element Types
export type ElementType =
  | 'source'
  | 'stream'
  | 'table'
  | 'trigger'
  | 'window'
  | 'filter'
  | 'projection'
  | 'aggregation'
  | 'groupBy'
  | 'join'
  | 'pattern'
  | 'partition'
  | 'sink';

export type ElementProperties =
  | SourceProperties
  | StreamProperties
  | TableProperties
  | TriggerProperties
  | WindowProperties
  | FilterProperties
  | ProjectionProperties
  | AggregationProperties
  | GroupByProperties
  | JoinProperties
  | PatternProperties
  | PartitionProperties
  | SinkProperties;

// Source Properties. A source brings external data in.
export type SourceType = 'kafka' | 'http' | 'mqtt' | 'file' | 'timer' | 'websocket';

export interface SourceProperties {
  sourceName: string;
  sourceType: SourceType;
  config: Record<string, string>;
}

// Sink Properties. A sink sends data out.
export type SinkType = 'kafka' | 'http' | 'mqtt' | 'file' | 'log' | 'websocket';

export interface SinkProperties {
  sinkName: string;
  sinkType: SinkType;
  config: Record<string, string>;
}

// Stream Properties
export interface StreamProperties {
  streamName: string;
  attributes: AttributeDefinition[];
  withConfig?: Record<string, string>;
}

export interface AttributeDefinition {
  name: string;
  type: AttributeType;
}

export type AttributeType = 'INT' | 'LONG' | 'DOUBLE' | 'FLOAT' | 'STRING' | 'BOOL';

// Table Properties
export interface TableProperties {
  tableName: string;
  attributes: AttributeDefinition[];
  extension?: string;
  primaryKey?: string[];
  withConfig?: Record<string, string>;
}

// Trigger Properties
export interface TriggerProperties {
  triggerId: string;
  triggerType: 'start' | 'periodic' | 'cron';
  atEvery?: number;
  cronExpression?: string;
}

// Window Properties
export interface WindowProperties {
  windowType: WindowType;
  parameters: WindowParameters;
}

export type WindowType =
  | 'length'
  | 'lengthBatch'
  | 'time'
  | 'timeBatch'
  | 'tumbling'
  | 'sliding'
  | 'session'
  | 'externalTime'
  | 'externalTimeBatch'
  | 'sort';

export interface WindowParameters {
  count?: number;
  duration?: TimeInterval;
  slideInterval?: TimeInterval;
  timestampAttribute?: string;
  gapDuration?: TimeInterval;
  sortAttribute?: string;
  sortOrder?: 'asc' | 'desc';
}

export interface TimeInterval {
  value: number;
  unit: TimeUnit;
}

export type TimeUnit = 'MILLISECONDS' | 'SECONDS' | 'MINUTES' | 'HOURS' | 'DAYS';

// Filter Properties
export interface FilterProperties {
  condition: Expression;
}

// Projection Properties
export interface ProjectionProperties {
  selectList: OutputAttribute[];
  distinct?: boolean;
}

export interface OutputAttribute {
  expression: Expression;
  alias?: string;
}

// Aggregation Properties
export interface AggregationProperties {
  aggregations: AggregationFunction[];
}

export interface AggregationFunction {
  type: AggregationType;
  expression: Expression;
  alias: string;
}

export type AggregationType =
  | 'COUNT'
  | 'SUM'
  | 'AVG'
  | 'MIN'
  | 'MAX'
  | 'STDDEV'
  | 'VARIANCE'
  | 'FIRST'
  | 'LAST'
  | 'DISTINCTCOUNT';

// Group By Properties
export interface GroupByProperties {
  groupByAttributes: string[];
  havingCondition?: Expression;
}

// Join Properties
export interface JoinProperties {
  joinType: JoinType;
  onCondition: Expression | null;
  trigger?: 'left' | 'right' | 'all';
  within?: TimeInterval;
}

export type JoinType = 'inner' | 'left_outer' | 'right_outer' | 'full_outer';

// Pattern Properties
export interface PatternProperties {
  mode: 'pattern' | 'sequence';
  patternExpression: PatternExpression;
  withinConstraint?: WithinConstraint;
}

export interface PatternExpression {
  type: PatternExpressionType;
  streamAlias?: string;
  streamName?: string;
  filter?: Expression;
  countMin?: number;
  countMax?: number;
  operator?: 'and' | 'or' | 'next' | 'every' | 'not';
  left?: PatternExpression;
  right?: PatternExpression;
  operand?: PatternExpression;
  duration?: TimeInterval;
}

export type PatternExpressionType =
  | 'stream'
  | 'logical'
  | 'count'
  | 'every'
  | 'absent'
  | 'next'
  | 'grouped';

export interface WithinConstraint {
  type: 'time' | 'event_count';
  value: number;
  unit?: string;
}

// Partition Properties
export interface PartitionProperties {
  partitionBy: PartitionAttribute[];
}

export interface PartitionAttribute {
  attribute: string;
  streamName: string;
}

// Expression Types. One shape for every kind. Which fields are set depends on type.
export interface Expression {
  type: ExpressionType;
  // Constants
  constantType?: 'string' | 'int' | 'long' | 'double' | 'float' | 'bool' | 'null';
  constantValue?: unknown;
  // Variables
  variableName?: string;
  streamId?: string;
  index?: number | 'last';
  // Functions
  functionName?: string;
  functionNamespace?: string;
  parameters?: Expression[];
  // Binary operations
  operator?: string;
  left?: Expression;
  right?: Expression;
  // Unary operations
  operand?: Expression;
  // CASE expressions
  caseOperand?: Expression;
  whenClauses?: WhenClause[];
  elseResult?: Expression;
  // CAST
  targetType?: AttributeType;
}

export type ExpressionType =
  | 'constant'
  | 'variable'
  | 'indexed_variable'
  | 'function'
  | 'add'
  | 'subtract'
  | 'multiply'
  | 'divide'
  | 'mod'
  | 'and'
  | 'or'
  | 'not'
  | 'compare'
  | 'in'
  | 'is_null'
  | 'case'
  | 'cast';

export interface WhenClause {
  condition: Expression;
  result: Expression;
}

// Configuration
export interface StudioConfig {
  gridSize: number;
  snapToGrid: boolean;
  autoSave: boolean;
  engineHost: string;
  enginePort: number;
}
