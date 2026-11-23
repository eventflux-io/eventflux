import { useMemo } from 'react';
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
  Trash2,
  Plus,
  ChevronDown,
} from 'lucide-react';
import { useApplicationStore, type UpstreamAttribute } from '../../stores/applicationStore';
import type { ElementType, AttributeDefinition, AttributeType } from '../../types';
import {
  getSourceTypes,
  getSinkTypes,
  getSourceSchema,
  getSinkSchema,
  getConnectorParameters,
  formatParameterName,
  getFunctionNames,
  getAggregatorTypes,
  getWindowTypes,
  getTableExtensions,
} from '../../schemas';

const elementIcons: Record<ElementType, React.ComponentType<{ className?: string }>> = {
  source: ArrowDownToLine,
  sink: ArrowUpFromLine,
  stream: Database,
  table: Table2,
  trigger: Timer,
  window: Clock,
  filter: Filter,
  projection: Columns,
  aggregation: BarChart2,
  groupBy: Group,
  join: GitMerge,
  pattern: Route,
  partition: Layers,
};

const elementNames: Record<ElementType, string> = {
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

const attributeTypes: AttributeType[] = ['INT', 'LONG', 'DOUBLE', 'FLOAT', 'STRING', 'BOOL'];

// Attribute dropdown with a "Custom..." choice that switches to a text box.
// Picking "Custom..." sets the value to '__custom__', which shows an empty text box.
function AttributeSelect({
  value,
  onChange,
  attributes,
  placeholder = 'Select attribute',
  className = '',
  allowCustom = true,
}: {
  value: string;
  onChange: (value: string) => void;
  attributes: UpstreamAttribute[];
  placeholder?: string;
  className?: string;
  allowCustom?: boolean;
}) {
  const hasAttributes = attributes.length > 0;
  const isInList = attributes.some((a) => a.name === value || `${a.streamId}.${a.name}` === value);

  // Show the dropdown when there are attributes and the value is empty or in the list
  if (hasAttributes && (isInList || !value)) {
    return (
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className={`form-select ${className}`}
      >
        <option value="">{placeholder}</option>
        {attributes.map((attr, i) => {
          const fullName = attr.streamId ? `${attr.streamId}.${attr.name}` : attr.name;
          return (
            <option key={i} value={attr.name}>
              {fullName} ({attr.type})
            </option>
          );
        })}
        {allowCustom && <option value="__custom__">Custom...</option>}
      </select>
    );
  }

  // Otherwise show a text box. The button clears the value to go back to the dropdown.
  return (
    <div className={`flex items-center gap-1 ${className}`}>
      <input
        type="text"
        value={value === '__custom__' ? '' : value}
        onChange={(e) => onChange(e.target.value)}
        className="form-input flex-1"
        placeholder={placeholder}
      />
      {hasAttributes && (
        <button
          onClick={() => onChange('')}
          className="p-1 text-gray-500 hover:text-gray-300"
          title="Select from list"
        >
          <ChevronDown className="w-3 h-3" />
        </button>
      )}
    </div>
  );
}

// Collapsible function hints panel
function FunctionHints({ show = false }: { show?: boolean }) {
  const functionNames = useMemo(() => getFunctionNames(), []);
  const aggregatorNames = useMemo(() => getAggregatorTypes(), []);

  if (!show) return null;

  return (
    <div className="mt-2 p-2 bg-gray-800/30 rounded text-xs">
      <div className="text-gray-400 mb-1">Available functions:</div>
      <div className="flex flex-wrap gap-1">
        {functionNames.slice(0, 15).map((fn) => (
          <span key={fn} className="px-1.5 py-0.5 bg-gray-700 rounded text-gray-300 font-mono">
            {fn}
          </span>
        ))}
        {functionNames.length > 15 && (
          <span className="px-1.5 py-0.5 text-gray-500">+{functionNames.length - 15} more</span>
        )}
      </div>
      <div className="text-gray-400 mt-2 mb-1">Aggregators:</div>
      <div className="flex flex-wrap gap-1">
        {aggregatorNames.map((fn) => (
          <span key={fn} className="px-1.5 py-0.5 bg-indigo-900/50 rounded text-indigo-300 font-mono">
            {fn}
          </span>
        ))}
      </div>
    </div>
  );
}

export function PropertiesPanel() {
  const { nodes, selectedElementIds, updateElement, removeElement, getUpstreamSchema, getAllStreams } = useApplicationStore();

  const selectedNode = useMemo(() => {
    if (selectedElementIds.length !== 1) return null;
    return nodes.find((n) => n.id === selectedElementIds[0]) || null;
  }, [nodes, selectedElementIds]);

  // Attributes that flow into the selected element from upstream nodes
  const upstreamSchema = useMemo(() => {
    if (!selectedNode) return [];
    return getUpstreamSchema(selectedNode.id);
  }, [selectedNode, getUpstreamSchema]);

  const allStreams = useMemo(() => {
    return getAllStreams();
  }, [getAllStreams]);

  if (!selectedNode) {
    return (
      <div className="flex-1 flex flex-col">
        <div className="flex-1 flex items-center justify-center text-sm text-gray-500 p-4 text-center">
          Select an element to view and edit its properties
        </div>
      </div>
    );
  }

  const Icon = elementIcons[selectedNode.type as ElementType] || Database;
  const typeName = elementNames[selectedNode.type as ElementType] || selectedNode.type;

  const handleUpdate = (newData: Record<string, unknown>) => {
    // The store merges newData into the node data. Keys left out are kept, not removed.
    updateElement(selectedNode.id, { data: newData });
  };

  const handleDelete = () => {
    removeElement(selectedNode.id);
  };

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      {/* Header */}
      <div className="px-3 py-2 border-b border-vscode-border flex items-center gap-2">
        <Icon className="w-4 h-4 text-gray-400" />
        <h2 className="text-sm font-medium text-gray-300 flex-1">{typeName}</h2>
        <button
          onClick={handleDelete}
          className="p-1 text-gray-500 hover:text-red-400 transition-colors"
          title="Delete element"
        >
          <Trash2 className="w-4 h-4" />
        </button>
      </div>

      {/* Properties */}
      <div className="flex-1 overflow-y-auto p-3">
        <PropertyEditor
          type={selectedNode.type as ElementType}
          data={selectedNode.data as Record<string, unknown>}
          onChange={handleUpdate}
          upstreamSchema={upstreamSchema}
          allStreams={allStreams}
        />
      </div>
    </div>
  );
}

interface PropertyEditorProps {
  type: ElementType;
  data: Record<string, unknown>;
  onChange: (changes: Record<string, unknown>) => void;
  upstreamSchema: UpstreamAttribute[];
  allStreams: { id: string; name: string; attributes: UpstreamAttribute[] }[];
}

function PropertyEditor({ type, data, onChange, upstreamSchema, allStreams }: PropertyEditorProps) {
  switch (type) {
    case 'source':
      return <SourcePropertyEditor data={data} onChange={onChange} />;
    case 'sink':
      return <SinkPropertyEditor data={data} onChange={onChange} />;
    case 'stream':
      return <StreamPropertyEditor data={data} onChange={onChange} />;
    case 'table':
      return <TablePropertyEditor data={data} onChange={onChange} />;
    case 'trigger':
      return <TriggerPropertyEditor data={data} onChange={onChange} />;
    case 'window':
      return <WindowPropertyEditor data={data} onChange={onChange} />;
    case 'filter':
      return <FilterPropertyEditor data={data} onChange={onChange} upstreamSchema={upstreamSchema} />;
    case 'projection':
      return <ProjectionPropertyEditor data={data} onChange={onChange} upstreamSchema={upstreamSchema} />;
    case 'aggregation':
      return <AggregationPropertyEditor data={data} onChange={onChange} upstreamSchema={upstreamSchema} />;
    case 'groupBy':
      return <GroupByPropertyEditor data={data} onChange={onChange} upstreamSchema={upstreamSchema} />;
    case 'join':
      return <JoinPropertyEditor data={data} onChange={onChange} allStreams={allStreams} />;
    case 'pattern':
      return <PatternPropertyEditor data={data} onChange={onChange} allStreams={allStreams} />;
    case 'partition':
      return <PartitionPropertyEditor data={data} onChange={onChange} allStreams={allStreams} />;
    default:
      return (
        <div className="text-sm text-gray-500">
          Property editor for {type} not yet implemented
        </div>
      );
  }
}

// Source connector choices built from the connector schemas
function getSourceConnectorTypes() {
  return getSourceTypes().map(type => {
    const schema = getSourceSchema(type);
    return {
      id: type,
      label: type.charAt(0).toUpperCase() + type.slice(1),
      schema,
    };
  });
}

// Sink connector choices built from the connector schemas
function getSinkConnectorTypes() {
  return getSinkTypes().map(type => {
    const schema = getSinkSchema(type);
    return {
      id: type,
      label: type.charAt(0).toUpperCase() + type.slice(1),
      schema,
    };
  });
}

// Source property editor. A source brings external data into the app.
function SourcePropertyEditor({ data, onChange }: { data: Record<string, unknown>; onChange: (changes: Record<string, unknown>) => void }) {
  const sourceName = (data.sourceName as string) || '';
  const sourceType = (data.sourceType as string) || getSourceTypes()[0] || 'rabbitmq';
  const config = (data.config as Record<string, string>) || {};

  const sourceConnectorTypes = getSourceConnectorTypes();
  const currentSchema = getSourceSchema(sourceType);
  const schemaParams = currentSchema ? getConnectorParameters(currentSchema) : [];

  const handleSourceTypeChange = (type: string) => {
    // Drop the old config and start with an empty entry for each required parameter
    const schema = getSourceSchema(type);
    const newConfig: Record<string, string> = {};
    if (schema) {
      for (const param of schema.requiredParameters || []) {
        newConfig[param] = '';
      }
    }
    onChange({
      ...data,
      sourceType: type,
      config: newConfig,
    });
  };

  const handleConfigChange = (key: string, value: string) => {
    onChange({ ...data, config: { ...config, [key]: value } });
  };

  const handleAddConfig = () => {
    const newKey = `config${Object.keys(config).length}`;
    onChange({ ...data, config: { ...config, [newKey]: '' } });
  };

  const handleRemoveConfig = (key: string) => {
    const newConfig = { ...config };
    delete newConfig[key];
    onChange({ ...data, config: newConfig });
  };

  // Keys the schema does not define, required or optional, show as custom config
  const schemaParamKeys = new Set(schemaParams.map(p => p.key));
  const customConfigKeys = Object.keys(config).filter(k => !schemaParamKeys.has(k));

  const isNameEmpty = !sourceName.trim();

  return (
    <div className="space-y-4">
      <div className="form-group">
        <label className="form-label form-label-required">Source Name</label>
        <input
          type="text"
          value={sourceName}
          onChange={(e) => onChange({ ...data, sourceName: e.target.value })}
          className={`form-input ${isNameEmpty ? 'form-input-error' : ''}`}
          placeholder="MySource"
        />
        {isNameEmpty && <div className="form-error">Source name is required</div>}
      </div>

      <div className="form-group">
        <label className="form-label">Connector Type</label>
        <select
          value={sourceType}
          onChange={(e) => handleSourceTypeChange(e.target.value)}
          className="form-select"
        >
          {sourceConnectorTypes.map((c) => (
            <option key={c.id} value={c.id}>{c.label}</option>
          ))}
        </select>
      </div>

      {/* Parameters defined by the schema */}
      {schemaParams.length > 0 && (
        <div className="form-group">
          <label className="form-label">Parameters</label>
          <div className="space-y-2">
            {schemaParams.map((param) => (
              <div key={param.key} className="flex items-center gap-2">
                <label className="text-xs text-gray-400 w-28 truncate" title={param.key}>
                  {formatParameterName(param.key)}
                  {param.required && <span className="text-red-400 ml-1">*</span>}
                </label>
                <input
                  type="text"
                  value={config[param.key] || ''}
                  onChange={(e) => handleConfigChange(param.key, e.target.value)}
                  className="form-input flex-1 text-xs"
                  placeholder={param.key}
                />
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Custom config, for keys the schema does not define */}
      <div className="form-group">
        <div className="flex items-center justify-between mb-2">
          <label className="form-label mb-0">Custom Config</label>
          <button
            onClick={handleAddConfig}
            className="p-1 text-gray-400 hover:text-white transition-colors"
            title="Add config"
          >
            <Plus className="w-4 h-4" />
          </button>
        </div>

        <div className="space-y-2">
          {customConfigKeys.map((key) => (
            <div key={key} className="flex items-center gap-1">
              <input
                type="text"
                value={key}
                onChange={(e) => {
                  const newConfig = { ...config };
                  const value = newConfig[key];
                  delete newConfig[key];
                  newConfig[e.target.value] = value;
                  onChange({ ...data, config: newConfig });
                }}
                className="form-input w-28 text-xs"
                placeholder="key"
              />
              <span className="text-gray-500">=</span>
              <input
                type="text"
                value={config[key] || ''}
                onChange={(e) => handleConfigChange(key, e.target.value)}
                className="form-input flex-1 text-xs"
                placeholder="value"
              />
              <button
                onClick={() => handleRemoveConfig(key)}
                className="p-1 text-gray-500 hover:text-red-400 transition-colors"
              >
                <Trash2 className="w-3 h-3" />
              </button>
            </div>
          ))}
          {customConfigKeys.length === 0 && (
            <div className="text-xs text-gray-600 italic">No custom config</div>
          )}
        </div>
      </div>

      <div className="text-xs text-gray-500 p-2 bg-gray-800/50 rounded">
        Connect this Source to a Stream to define where events flow into the system.
      </div>
    </div>
  );
}

// Sink property editor. A sink sends data out of the app.
function SinkPropertyEditor({ data, onChange }: { data: Record<string, unknown>; onChange: (changes: Record<string, unknown>) => void }) {
  const sinkName = (data.sinkName as string) || '';
  const sinkType = (data.sinkType as string) || getSinkTypes()[0] || 'log';
  const config = (data.config as Record<string, string>) || {};

  const sinkConnectorTypes = getSinkConnectorTypes();
  const currentSchema = getSinkSchema(sinkType);
  const schemaParams = currentSchema ? getConnectorParameters(currentSchema) : [];

  const handleSinkTypeChange = (type: string) => {
    // Drop the old config and start with an empty entry for each required parameter
    const schema = getSinkSchema(type);
    const newConfig: Record<string, string> = {};
    if (schema) {
      for (const param of schema.requiredParameters || []) {
        newConfig[param] = '';
      }
    }
    onChange({
      ...data,
      sinkType: type,
      config: newConfig,
    });
  };

  const handleConfigChange = (key: string, value: string) => {
    onChange({ ...data, config: { ...config, [key]: value } });
  };

  const handleAddConfig = () => {
    const newKey = `config${Object.keys(config).length}`;
    onChange({ ...data, config: { ...config, [newKey]: '' } });
  };

  const handleRemoveConfig = (key: string) => {
    const newConfig = { ...config };
    delete newConfig[key];
    onChange({ ...data, config: newConfig });
  };

  // Keys the schema does not define, required or optional, show as custom config
  const schemaParamKeys = new Set(schemaParams.map(p => p.key));
  const customConfigKeys = Object.keys(config).filter(k => !schemaParamKeys.has(k));

  const isNameEmpty = !sinkName.trim();

  return (
    <div className="space-y-4">
      <div className="form-group">
        <label className="form-label form-label-required">Sink Name</label>
        <input
          type="text"
          value={sinkName}
          onChange={(e) => onChange({ ...data, sinkName: e.target.value })}
          className={`form-input ${isNameEmpty ? 'form-input-error' : ''}`}
          placeholder="MySink"
        />
        {isNameEmpty && <div className="form-error">Sink name is required</div>}
      </div>

      <div className="form-group">
        <label className="form-label">Connector Type</label>
        <select
          value={sinkType}
          onChange={(e) => handleSinkTypeChange(e.target.value)}
          className="form-select"
        >
          {sinkConnectorTypes.map((c) => (
            <option key={c.id} value={c.id}>{c.label}</option>
          ))}
        </select>
      </div>

      {/* Parameters defined by the schema */}
      {schemaParams.length > 0 && (
        <div className="form-group">
          <label className="form-label">Parameters</label>
          <div className="space-y-2">
            {schemaParams.map((param) => (
              <div key={param.key} className="flex items-center gap-2">
                <label className="text-xs text-gray-400 w-28 truncate" title={param.key}>
                  {formatParameterName(param.key)}
                  {param.required && <span className="text-red-400 ml-1">*</span>}
                </label>
                <input
                  type="text"
                  value={config[param.key] || ''}
                  onChange={(e) => handleConfigChange(param.key, e.target.value)}
                  className="form-input flex-1 text-xs"
                  placeholder={param.key}
                />
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Custom config, for keys the schema does not define */}
      <div className="form-group">
        <div className="flex items-center justify-between mb-2">
          <label className="form-label mb-0">Custom Config</label>
          <button
            onClick={handleAddConfig}
            className="p-1 text-gray-400 hover:text-white transition-colors"
            title="Add config"
          >
            <Plus className="w-4 h-4" />
          </button>
        </div>

        <div className="space-y-2">
          {customConfigKeys.map((key) => (
            <div key={key} className="flex items-center gap-1">
              <input
                type="text"
                value={key}
                onChange={(e) => {
                  const newConfig = { ...config };
                  const value = newConfig[key];
                  delete newConfig[key];
                  newConfig[e.target.value] = value;
                  onChange({ ...data, config: newConfig });
                }}
                className="form-input w-28 text-xs"
                placeholder="key"
              />
              <span className="text-gray-500">=</span>
              <input
                type="text"
                value={config[key] || ''}
                onChange={(e) => handleConfigChange(key, e.target.value)}
                className="form-input flex-1 text-xs"
                placeholder="value"
              />
              <button
                onClick={() => handleRemoveConfig(key)}
                className="p-1 text-gray-500 hover:text-red-400 transition-colors"
              >
                <Trash2 className="w-3 h-3" />
              </button>
            </div>
          ))}
          {customConfigKeys.length === 0 && (
            <div className="text-xs text-gray-600 italic">No custom config</div>
          )}
        </div>
      </div>

      <div className="text-xs text-gray-500 p-2 bg-gray-800/50 rounded">
        Connect a Stream or Output to this Sink to define where events flow out of the system.
      </div>
    </div>
  );
}

// Stream property editor. A stream carries events between elements.
function StreamPropertyEditor({ data, onChange }: { data: Record<string, unknown>; onChange: (changes: Record<string, unknown>) => void }) {
  const streamName = (data.streamName as string) || '';
  const attributes = (data.attributes as AttributeDefinition[]) || [];

  const handleNameChange = (name: string) => {
    onChange({ ...data, streamName: name });
  };

  const handleAttributeChange = (index: number, field: 'name' | 'type', value: string) => {
    const newAttributes = [...attributes];
    newAttributes[index] = { ...newAttributes[index], [field]: value };
    onChange({ ...data, attributes: newAttributes });
  };

  const handleAddAttribute = () => {
    onChange({
      ...data,
      attributes: [...attributes, { name: `attr${attributes.length}`, type: 'STRING' as AttributeType }],
    });
  };

  const handleRemoveAttribute = (index: number) => {
    onChange({
      ...data,
      attributes: attributes.filter((_, i) => i !== index),
    });
  };

  const isNameEmpty = !streamName.trim();

  return (
    <div className="space-y-4">
      <div className="form-group">
        <label className="form-label form-label-required">Stream Name</label>
        <input
          type="text"
          value={streamName}
          onChange={(e) => handleNameChange(e.target.value)}
          className={`form-input ${isNameEmpty ? 'form-input-error' : ''}`}
          placeholder="StreamName"
        />
        {isNameEmpty && <div className="form-error">Stream name is required</div>}
      </div>

      <div className="form-group">
        <div className="flex items-center justify-between mb-2">
          <label className="form-label mb-0">Schema</label>
          <button
            onClick={handleAddAttribute}
            className="p-1 text-gray-400 hover:text-white transition-colors"
            title="Add attribute"
          >
            <Plus className="w-4 h-4" />
          </button>
        </div>

        <div className="space-y-2">
          {attributes.map((attr, index) => (
            <div key={index} className="flex items-center gap-2">
              <input
                type="text"
                value={attr.name}
                onChange={(e) => handleAttributeChange(index, 'name', e.target.value)}
                className="form-input flex-1"
                placeholder="name"
              />
              <select
                value={attr.type}
                onChange={(e) => handleAttributeChange(index, 'type', e.target.value)}
                className="form-select w-24"
              >
                {attributeTypes.map((t) => (
                  <option key={t} value={t}>{t}</option>
                ))}
              </select>
              <button
                onClick={() => handleRemoveAttribute(index)}
                className="p-1 text-gray-500 hover:text-red-400 transition-colors"
              >
                <Trash2 className="w-3 h-3" />
              </button>
            </div>
          ))}
        </div>
      </div>

      <div className="text-xs text-gray-500 p-2 bg-gray-800/50 rounded">
        Streams are central data channels. Connect a Source to feed events in, or query results via INSERT INTO.
      </div>
    </div>
  );
}

// Table property editor
function TablePropertyEditor({ data, onChange }: { data: Record<string, unknown>; onChange: (changes: Record<string, unknown>) => void }) {
  const availableTableExtensions = getTableExtensions();
  const tableName = (data.tableName as string) || '';
  const attributes = (data.attributes as AttributeDefinition[]) || [];
  const extension = (data.extension as string) || '';
  const primaryKey = (data.primaryKey as string[]) || [];
  const withConfig = (data.withConfig as Record<string, string>) || {};

  const extensionDisplayNames: Record<string, string> = {
    inMemory: 'In-Memory',
    cache: 'Cache (Redis)',
    jdbc: 'JDBC Database',
  };

  const handleNameChange = (name: string) => {
    onChange({ ...data, tableName: name });
  };

  const handleAttributeChange = (index: number, field: 'name' | 'type', value: string) => {
    const newAttributes = [...attributes];
    newAttributes[index] = { ...newAttributes[index], [field]: value };
    onChange({ ...data, attributes: newAttributes });
  };

  const handleAddAttribute = () => {
    onChange({
      ...data,
      attributes: [...attributes, { name: `attr${attributes.length}`, type: 'STRING' as AttributeType }],
    });
  };

  const handleRemoveAttribute = (index: number) => {
    onChange({
      ...data,
      attributes: attributes.filter((_, i) => i !== index),
    });
  };

  const handlePrimaryKeyChange = (attrName: string, checked: boolean) => {
    if (checked) {
      onChange({ ...data, primaryKey: [...primaryKey, attrName] });
    } else {
      onChange({ ...data, primaryKey: primaryKey.filter((k) => k !== attrName) });
    }
  };

  const handleExtensionChange = (ext: string) => {
    // Replace withConfig with starter values for cache and jdbc. In memory tables get none.
    let defaultConfig: Record<string, string> = {};
    if (ext === 'cache') {
      defaultConfig = {
        'cache.host': 'localhost',
        'cache.port': '6379',
      };
    } else if (ext === 'jdbc') {
      defaultConfig = {
        'jdbc.url': 'jdbc:mysql://localhost:3306/db',
        'jdbc.driver': 'com.mysql.cj.jdbc.Driver',
      };
    }
    onChange({ ...data, extension: ext || undefined, withConfig: ext && ext !== 'inMemory' ? defaultConfig : undefined });
  };

  const handleConfigChange = (key: string, value: string) => {
    onChange({ ...data, withConfig: { ...withConfig, [key]: value } });
  };

  const handleAddConfig = () => {
    const newKey = `config${Object.keys(withConfig).length}`;
    onChange({ ...data, withConfig: { ...withConfig, [newKey]: '' } });
  };

  const handleRemoveConfig = (key: string) => {
    const newConfig = { ...withConfig };
    delete newConfig[key];
    onChange({ ...data, withConfig: Object.keys(newConfig).length > 0 ? newConfig : undefined });
  };

  const isNameEmpty = !tableName.trim();

  return (
    <div className="space-y-4">
      <div className="form-group">
        <label className="form-label form-label-required">Table Name</label>
        <input
          type="text"
          value={tableName}
          onChange={(e) => handleNameChange(e.target.value)}
          className={`form-input ${isNameEmpty ? 'form-input-error' : ''}`}
          placeholder="TableName"
        />
        {isNameEmpty && <div className="form-error">Table name is required</div>}
      </div>

      <div className="form-group">
        <label className="form-label">Extension (optional)</label>
        <select
          value={extension}
          onChange={(e) => handleExtensionChange(e.target.value)}
          className="form-select"
        >
          <option value="">In-Memory (default)</option>
          {availableTableExtensions
            .filter((ext) => ext !== 'inMemory') // inMemory is the default, shown as the empty choice
            .map((ext) => (
              <option key={ext} value={ext}>
                {extensionDisplayNames[ext] || ext}
              </option>
            ))}
        </select>
      </div>

      <div className="form-group">
        <div className="flex items-center justify-between mb-2">
          <label className="form-label mb-0">Schema</label>
          <button
            onClick={handleAddAttribute}
            className="p-1 text-gray-400 hover:text-white transition-colors"
          >
            <Plus className="w-4 h-4" />
          </button>
        </div>

        <div className="space-y-2">
          {attributes.map((attr, index) => (
            <div key={index} className="flex items-center gap-2">
              <input
                type="checkbox"
                checked={primaryKey.includes(attr.name)}
                onChange={(e) => handlePrimaryKeyChange(attr.name, e.target.checked)}
                className="form-checkbox w-4 h-4"
                title="Primary Key"
              />
              <input
                type="text"
                value={attr.name}
                onChange={(e) => handleAttributeChange(index, 'name', e.target.value)}
                className="form-input flex-1"
                placeholder="name"
              />
              <select
                value={attr.type}
                onChange={(e) => handleAttributeChange(index, 'type', e.target.value)}
                className="form-select w-24"
              >
                {attributeTypes.map((t) => (
                  <option key={t} value={t}>{t}</option>
                ))}
              </select>
              <button
                onClick={() => handleRemoveAttribute(index)}
                className="p-1 text-gray-500 hover:text-red-400 transition-colors"
              >
                <Trash2 className="w-3 h-3" />
              </button>
            </div>
          ))}
        </div>
        {primaryKey.length > 0 && (
          <div className="text-xs text-gray-500 mt-1">
            Primary Key: {primaryKey.join(', ')}
          </div>
        )}
      </div>

      {extension && (
        <div className="form-group">
          <div className="flex items-center justify-between mb-2">
            <label className="form-label mb-0">Configuration</label>
            <button
              onClick={handleAddConfig}
              className="p-1 text-gray-400 hover:text-white transition-colors"
            >
              <Plus className="w-4 h-4" />
            </button>
          </div>

          <div className="space-y-2">
            {Object.entries(withConfig).map(([key, value]) => (
              <div key={key} className="flex items-center gap-2">
                <input
                  type="text"
                  value={key}
                  onChange={(e) => {
                    const newConfig = { ...withConfig };
                    delete newConfig[key];
                    newConfig[e.target.value] = value;
                    onChange({ ...data, withConfig: newConfig });
                  }}
                  className="form-input w-32"
                  placeholder="key"
                />
                <span className="text-gray-500">=</span>
                <input
                  type="text"
                  value={value}
                  onChange={(e) => handleConfigChange(key, e.target.value)}
                  className="form-input flex-1"
                  placeholder="value"
                />
                <button
                  onClick={() => handleRemoveConfig(key)}
                  className="p-1 text-gray-500 hover:text-red-400 transition-colors"
                >
                  <Trash2 className="w-3 h-3" />
                </button>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

// Trigger Property Editor
function TriggerPropertyEditor({ data, onChange }: { data: Record<string, unknown>; onChange: (changes: Record<string, unknown>) => void }) {
  const triggerId = (data.triggerId as string) || '';
  const triggerType = (data.triggerType as string) || 'periodic';
  const atEvery = (data.atEvery as number) || 1000;
  const cronExpression = (data.cronExpression as string) || '';

  return (
    <div className="space-y-4">
      <div className="form-group">
        <label className="form-label">Trigger Name</label>
        <input
          type="text"
          value={triggerId}
          onChange={(e) => onChange({ ...data, triggerId: e.target.value })}
          className="form-input"
          placeholder="TriggerName"
        />
      </div>

      <div className="form-group">
        <label className="form-label">Type</label>
        <select
          value={triggerType}
          onChange={(e) => onChange({ ...data, triggerType: e.target.value })}
          className="form-select"
        >
          <option value="start">Start (Once)</option>
          <option value="periodic">Periodic</option>
          <option value="cron">Cron</option>
        </select>
      </div>

      {triggerType === 'periodic' && (
        <div className="form-group">
          <label className="form-label">Interval (ms)</label>
          <input
            type="number"
            value={atEvery}
            onChange={(e) => onChange({ ...data, atEvery: parseInt(e.target.value) || 1000 })}
            className="form-input"
            min={1}
          />
        </div>
      )}

      {triggerType === 'cron' && (
        <div className="form-group">
          <label className="form-label">Cron Expression</label>
          <input
            type="text"
            value={cronExpression}
            onChange={(e) => onChange({ ...data, cronExpression: e.target.value })}
            className="form-input"
            placeholder="*/5 * * * * *"
          />
        </div>
      )}
    </div>
  );
}

// Window Property Editor
function WindowPropertyEditor({ data, onChange }: { data: Record<string, unknown>; onChange: (changes: Record<string, unknown>) => void }) {
  const availableWindowTypes = getWindowTypes();
  const windowType = (data.windowType as string) || availableWindowTypes[0] || 'length';
  const parameters = (data.parameters as Record<string, unknown>) || {};

  const handleTypeChange = (type: string) => {
    onChange({ ...data, windowType: type, parameters: getDefaultParameters(type) });
  };

  const handleParamChange = (param: string, value: unknown) => {
    onChange({ ...data, parameters: { ...parameters, [param]: value } });
  };

  // Turns a window type into a display name, such as "lengthBatch" into "Length Batch"
  const formatWindowTypeName = (type: string): string => {
    return type
      .replace(/([A-Z])/g, ' $1')
      .replace(/^./, (str) => str.toUpperCase())
      .trim();
  };

  return (
    <div className="space-y-4">
      <div className="form-group">
        <label className="form-label">Window Type</label>
        <select
          value={windowType}
          onChange={(e) => handleTypeChange(e.target.value)}
          className="form-select"
        >
          {availableWindowTypes.map((type) => (
            <option key={type} value={type}>{formatWindowTypeName(type)}</option>
          ))}
        </select>
      </div>

      {/* Count parameter */}
      {['length', 'lengthBatch', 'sort'].includes(windowType) && (
        <div className="form-group">
          <label className="form-label">Count</label>
          <input
            type="number"
            value={(parameters.count as number) || 10}
            onChange={(e) => handleParamChange('count', parseInt(e.target.value) || 10)}
            className="form-input"
            min={1}
          />
        </div>
      )}

      {/* Duration parameter */}
      {['time', 'timeBatch', 'tumbling', 'session', 'externalTime', 'externalTimeBatch'].includes(windowType) && (
        <DurationInput
          label="Duration"
          value={parameters.duration as { value: number; unit: string } | undefined}
          onChange={(duration) => handleParamChange('duration', duration)}
        />
      )}

      {/* Slide interval for sliding window */}
      {windowType === 'sliding' && (
        <>
          <DurationInput
            label="Window Size"
            value={parameters.duration as { value: number; unit: string } | undefined}
            onChange={(duration) => handleParamChange('duration', duration)}
          />
          <DurationInput
            label="Slide Interval"
            value={parameters.slideInterval as { value: number; unit: string } | undefined}
            onChange={(slideInterval) => handleParamChange('slideInterval', slideInterval)}
          />
        </>
      )}

      {/* Timestamp attribute for external time */}
      {['externalTime', 'externalTimeBatch'].includes(windowType) && (
        <div className="form-group">
          <label className="form-label">Timestamp Attribute</label>
          <input
            type="text"
            value={(parameters.timestampAttribute as string) || ''}
            onChange={(e) => handleParamChange('timestampAttribute', e.target.value)}
            className="form-input"
            placeholder="timestamp"
          />
        </div>
      )}

      {/* Sort attribute */}
      {windowType === 'sort' && (
        <div className="form-group">
          <label className="form-label">Sort Attribute</label>
          <input
            type="text"
            value={(parameters.sortAttribute as string) || ''}
            onChange={(e) => handleParamChange('sortAttribute', e.target.value)}
            className="form-input"
            placeholder="price"
          />
        </div>
      )}
    </div>
  );
}

// Filter Property Editor
function FilterPropertyEditor({ data, onChange, upstreamSchema }: { data: Record<string, unknown>; onChange: (changes: Record<string, unknown>) => void; upstreamSchema: UpstreamAttribute[] }) {
  const condition = data.condition as Record<string, unknown> | null;

  // The filter is a single comparison for now. The operator defaults to '>'.
  const leftVar = (condition?.left as Record<string, unknown>)?.variableName as string || '';
  const operator = (condition?.operator as string) || '>';
  const rawRightValue = (condition?.right as Record<string, unknown>)?.constantValue;
  const rightValue = rawRightValue !== null && rawRightValue !== undefined ? String(rawRightValue) : '';

  const handleChange = (left: string, op: string, right: string) => {
    const rightNum = parseFloat(right);
    onChange({
      ...data,
      condition: {
        type: 'compare',
        operator: op,
        left: { type: 'variable', variableName: left },
        right: { type: 'constant', constantType: !isNaN(rightNum) ? 'double' : 'string', constantValue: !isNaN(rightNum) ? rightNum : right },
      },
    });
  };

  return (
    <div className="space-y-4">
      <div className="form-group">
        <label className="form-label">Condition</label>
        <div className="space-y-2">
          {/* Attribute selector, full width */}
          <AttributeSelect
            value={leftVar}
            onChange={(val) => handleChange(val, operator, rightValue)}
            attributes={upstreamSchema}
            placeholder="Select attribute"
            className="w-full"
          />

          {/* Operator and value on same row */}
          <div className="flex items-center gap-2">
            <select
              value={operator}
              onChange={(e) => handleChange(leftVar, e.target.value, rightValue)}
              className="form-select w-20"
            >
              <option value="=">=</option>
              <option value="!=">!=</option>
              <option value=">">&gt;</option>
              <option value="<">&lt;</option>
              <option value=">=">&gt;=</option>
              <option value="<=">&lt;=</option>
            </select>
            <input
              type="text"
              value={rightValue}
              onChange={(e) => handleChange(leftVar, operator, e.target.value)}
              className="form-input flex-1"
              placeholder="value to compare"
            />
          </div>
        </div>

        {/* Preview of the condition */}
        {leftVar && rightValue && (
          <div className="mt-2 p-2 bg-gray-800/50 rounded text-xs text-gray-300 font-mono">
            {leftVar} {operator} {rightValue}
          </div>
        )}
      </div>

      {upstreamSchema.length > 0 && (
        <div className="text-xs text-gray-500">
          Attributes: {upstreamSchema.map((a) => a.name).join(', ')}
        </div>
      )}

      <FunctionHints show={true} />
    </div>
  );
}

// Duration Input Component
function DurationInput({
  label,
  value,
  onChange,
}: {
  label: string;
  value: { value: number; unit: string } | undefined;
  onChange: (value: { value: number; unit: string }) => void;
}) {
  const duration = value || { value: 1, unit: 'SECONDS' };

  return (
    <div className="form-group">
      <label className="form-label">{label}</label>
      <div className="flex items-center gap-2">
        <input
          type="number"
          value={duration.value}
          onChange={(e) => onChange({ ...duration, value: parseInt(e.target.value) || 1 })}
          className="form-input flex-1"
          min={1}
        />
        <select
          value={duration.unit}
          onChange={(e) => onChange({ ...duration, unit: e.target.value })}
          className="form-select w-28"
        >
          <option value="MILLISECONDS">ms</option>
          <option value="SECONDS">sec</option>
          <option value="MINUTES">min</option>
          <option value="HOURS">hour</option>
          <option value="DAYS">day</option>
        </select>
      </div>
    </div>
  );
}

function getDefaultParameters(windowType: string): Record<string, unknown> {
  switch (windowType) {
    case 'length':
    case 'lengthBatch':
      return { count: 10 };
    case 'time':
    case 'timeBatch':
    case 'tumbling':
      return { duration: { value: 5, unit: 'SECONDS' } };
    case 'sliding':
      return { duration: { value: 10, unit: 'SECONDS' }, slideInterval: { value: 2, unit: 'SECONDS' } };
    case 'session':
      return { gapDuration: { value: 30, unit: 'SECONDS' } };
    case 'externalTime':
    case 'externalTimeBatch':
      return { timestampAttribute: 'timestamp', duration: { value: 5, unit: 'SECONDS' } };
    case 'sort':
      return { count: 10, sortAttribute: 'value' };
    default:
      return {};
  }
}

// Projection Property Editor
function ProjectionPropertyEditor({ data, onChange, upstreamSchema }: { data: Record<string, unknown>; onChange: (changes: Record<string, unknown>) => void; upstreamSchema: UpstreamAttribute[] }) {
  const selectList = (data.selectList as { expression: Record<string, unknown>; alias?: string }[]) || [];
  const distinct = (data.distinct as boolean) || false;

  const handleAddColumn = () => {
    onChange({
      ...data,
      selectList: [
        ...selectList,
        {
          expression: { type: 'variable', variableName: '' },
          alias: '',
        },
      ],
    });
  };

  const handleRemoveColumn = (index: number) => {
    onChange({
      ...data,
      selectList: selectList.filter((_, i) => i !== index),
    });
  };

  const handleColumnChange = (index: number, field: 'attribute' | 'alias', value: string) => {
    const newSelectList = [...selectList];
    if (field === 'attribute') {
      newSelectList[index] = {
        ...newSelectList[index],
        expression: { type: 'variable', variableName: value },
      };
    } else {
      newSelectList[index] = {
        ...newSelectList[index],
        alias: value || undefined,
      };
    }
    onChange({ ...data, selectList: newSelectList });
  };

  return (
    <div className="space-y-4">
      <div className="form-group">
        <label className="flex items-center gap-2">
          <input
            type="checkbox"
            checked={distinct}
            onChange={(e) => onChange({ ...data, distinct: e.target.checked })}
            className="form-checkbox"
          />
          <span className="text-xs text-gray-400">DISTINCT</span>
        </label>
      </div>

      <div className="form-group">
        <div className="flex items-center justify-between mb-2">
          <label className="form-label mb-0">Select Columns</label>
          <button
            onClick={handleAddColumn}
            className="p-1 text-gray-400 hover:text-white transition-colors"
            title="Add column"
          >
            <Plus className="w-4 h-4" />
          </button>
        </div>

        <div className="space-y-2">
          {selectList.length === 0 && (
            <div className="text-xs text-gray-500 italic">* (all columns)</div>
          )}
          {selectList.map((col, index) => (
            <div key={index} className="flex items-center gap-2">
              <AttributeSelect
                value={(col.expression as Record<string, unknown>)?.variableName as string || ''}
                onChange={(val) => handleColumnChange(index, 'attribute', val)}
                attributes={upstreamSchema}
                placeholder="attribute"
                className="flex-1"
              />
              <span className="text-gray-500 text-xs">AS</span>
              <input
                type="text"
                value={col.alias || ''}
                onChange={(e) => handleColumnChange(index, 'alias', e.target.value)}
                className="form-input flex-1"
                placeholder="alias"
              />
              <button
                onClick={() => handleRemoveColumn(index)}
                className="p-1 text-gray-500 hover:text-red-400 transition-colors"
              >
                <Trash2 className="w-3 h-3" />
              </button>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

// Aggregation Property Editor
function AggregationPropertyEditor({ data, onChange, upstreamSchema }: { data: Record<string, unknown>; onChange: (changes: Record<string, unknown>) => void; upstreamSchema: UpstreamAttribute[] }) {
  const aggregations = (data.aggregations as { type: string; expression: Record<string, unknown>; alias: string }[]) || [];

  const aggregationTypes = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'STDDEV', 'VARIANCE', 'FIRST', 'LAST', 'DISTINCTCOUNT'];

  // Offer * first so COUNT(*) can be picked
  const attributeOptions: UpstreamAttribute[] = [
    { name: '*', type: 'ALL', source: '' },
    ...upstreamSchema,
  ];

  const handleAddAggregation = () => {
    onChange({
      ...data,
      aggregations: [
        ...aggregations,
        {
          type: 'COUNT',
          expression: { type: 'variable', variableName: '*' },
          alias: `agg${aggregations.length}`,
        },
      ],
    });
  };

  const handleRemoveAggregation = (index: number) => {
    onChange({
      ...data,
      aggregations: aggregations.filter((_, i) => i !== index),
    });
  };

  const handleAggregationChange = (index: number, field: 'type' | 'attribute' | 'alias', value: string) => {
    const newAggregations = [...aggregations];
    if (field === 'attribute') {
      newAggregations[index] = {
        ...newAggregations[index],
        expression: { type: 'variable', variableName: value },
      };
    } else if (field === 'type') {
      newAggregations[index] = {
        ...newAggregations[index],
        type: value,
      };
    } else {
      newAggregations[index] = {
        ...newAggregations[index],
        alias: value,
      };
    }
    onChange({ ...data, aggregations: newAggregations });
  };

  return (
    <div className="space-y-4">
      <div className="form-group">
        <div className="flex items-center justify-between mb-2">
          <label className="form-label mb-0">Aggregations</label>
          <button
            onClick={handleAddAggregation}
            className="p-1 text-gray-400 hover:text-white transition-colors"
            title="Add aggregation"
          >
            <Plus className="w-4 h-4" />
          </button>
        </div>

        <div className="space-y-3">
          {aggregations.map((agg, index) => (
            <div key={index} className="p-2 bg-gray-800/50 rounded border border-gray-700">
              <div className="flex items-center gap-2 mb-2">
                <select
                  value={agg.type}
                  onChange={(e) => handleAggregationChange(index, 'type', e.target.value)}
                  className="form-select flex-1"
                >
                  {aggregationTypes.map((t) => (
                    <option key={t} value={t}>{t}</option>
                  ))}
                </select>
                <button
                  onClick={() => handleRemoveAggregation(index)}
                  className="p-1 text-gray-500 hover:text-red-400 transition-colors"
                >
                  <Trash2 className="w-3 h-3" />
                </button>
              </div>
              <div className="flex items-center gap-2">
                <AttributeSelect
                  value={(agg.expression as Record<string, unknown>)?.variableName as string || ''}
                  onChange={(val) => handleAggregationChange(index, 'attribute', val)}
                  attributes={attributeOptions}
                  placeholder="attribute or *"
                  className="flex-1"
                />
                <span className="text-gray-500 text-xs">AS</span>
                <input
                  type="text"
                  value={agg.alias || ''}
                  onChange={(e) => handleAggregationChange(index, 'alias', e.target.value)}
                  className="form-input flex-1"
                  placeholder="alias"
                />
              </div>
            </div>
          ))}
        </div>
      </div>

      <FunctionHints show={true} />
    </div>
  );
}

// Group By Property Editor
function GroupByPropertyEditor({ data, onChange, upstreamSchema }: { data: Record<string, unknown>; onChange: (changes: Record<string, unknown>) => void; upstreamSchema: UpstreamAttribute[] }) {
  const groupByAttributes = (data.groupByAttributes as string[]) || [];
  const havingCondition = data.havingCondition as Record<string, unknown> | null;

  const handleAddAttribute = () => {
    onChange({
      ...data,
      groupByAttributes: [...groupByAttributes, ''],
    });
  };

  const handleRemoveAttribute = (index: number) => {
    onChange({
      ...data,
      groupByAttributes: groupByAttributes.filter((_, i) => i !== index),
    });
  };

  const handleAttributeChange = (index: number, value: string) => {
    const newAttributes = [...groupByAttributes];
    newAttributes[index] = value;
    onChange({ ...data, groupByAttributes: newAttributes });
  };

  // HAVING is a single comparison, attribute operator value. The operator defaults to '>'.
  const havingLeft = (havingCondition?.left as Record<string, unknown>)?.variableName as string || '';
  const havingOp = (havingCondition?.operator as string) || '>';
  const rawHavingRight = (havingCondition?.right as Record<string, unknown>)?.constantValue;
  const havingRight = rawHavingRight !== null && rawHavingRight !== undefined ? String(rawHavingRight) : '';

  const handleHavingChange = (left: string, op: string, right: string) => {
    if (!left && !right) {
      onChange({ ...data, havingCondition: undefined });
      return;
    }
    const rightNum = parseFloat(right);
    onChange({
      ...data,
      havingCondition: {
        type: 'compare',
        operator: op,
        left: { type: 'variable', variableName: left },
        right: { type: 'constant', constantType: !isNaN(rightNum) ? 'double' : 'string', constantValue: !isNaN(rightNum) ? rightNum : right },
      },
    });
  };

  return (
    <div className="space-y-4">
      <div className="form-group">
        <div className="flex items-center justify-between mb-2">
          <label className="form-label mb-0">Group By Attributes</label>
          <button
            onClick={handleAddAttribute}
            className="p-1 text-gray-400 hover:text-white transition-colors"
            title="Add attribute"
          >
            <Plus className="w-4 h-4" />
          </button>
        </div>

        <div className="space-y-2">
          {groupByAttributes.map((attr, index) => (
            <div key={index} className="flex items-center gap-2">
              <AttributeSelect
                value={attr}
                onChange={(val) => handleAttributeChange(index, val)}
                attributes={upstreamSchema}
                placeholder="attribute"
                className="flex-1"
              />
              <button
                onClick={() => handleRemoveAttribute(index)}
                className="p-1 text-gray-500 hover:text-red-400 transition-colors"
              >
                <Trash2 className="w-3 h-3" />
              </button>
            </div>
          ))}
        </div>
      </div>

      <div className="form-group">
        <label className="form-label">HAVING Condition (optional)</label>
        <div className="flex items-center gap-2">
          <input
            type="text"
            value={havingLeft}
            onChange={(e) => handleHavingChange(e.target.value, havingOp, havingRight)}
            className="form-input flex-1"
            placeholder="agg alias"
          />
          <select
            value={havingOp}
            onChange={(e) => handleHavingChange(havingLeft, e.target.value, havingRight)}
            className="form-select w-16"
          >
            <option value="=">=</option>
            <option value="!=">!=</option>
            <option value=">">&gt;</option>
            <option value="<">&lt;</option>
            <option value=">=">&gt;=</option>
            <option value="<=">&lt;=</option>
          </select>
          <input
            type="text"
            value={havingRight}
            onChange={(e) => handleHavingChange(havingLeft, havingOp, e.target.value)}
            className="form-input flex-1"
            placeholder="value"
          />
        </div>
      </div>
    </div>
  );
}

// Join Property Editor
function JoinPropertyEditor({ data, onChange, allStreams }: { data: Record<string, unknown>; onChange: (changes: Record<string, unknown>) => void; allStreams: { id: string; name: string; attributes: UpstreamAttribute[] }[] }) {
  const joinType = (data.joinType as string) || 'inner';
  const onCondition = data.onCondition as Record<string, unknown> | null;
  const trigger = (data.trigger as string) || 'all';
  const within = data.within as { value: number; unit: string } | undefined;

  // The ON condition is a single equality, left.attr = right.attr
  const leftStream = (onCondition?.left as Record<string, unknown>)?.streamId as string || '';
  const leftAttr = (onCondition?.left as Record<string, unknown>)?.variableName as string || '';
  const rightStream = (onCondition?.right as Record<string, unknown>)?.streamId as string || '';
  const rightAttr = (onCondition?.right as Record<string, unknown>)?.variableName as string || '';

  const leftStreamAttrs = allStreams.find((s) => s.name === leftStream)?.attributes || [];
  const rightStreamAttrs = allStreams.find((s) => s.name === rightStream)?.attributes || [];

  const handleConditionChange = (ls: string, la: string, rs: string, ra: string) => {
    if (!la && !ra) {
      onChange({ ...data, onCondition: null });
      return;
    }
    onChange({
      ...data,
      onCondition: {
        type: 'compare',
        operator: '=',
        left: { type: 'variable', streamId: ls || undefined, variableName: la },
        right: { type: 'variable', streamId: rs || undefined, variableName: ra },
      },
    });
  };

  return (
    <div className="space-y-4">
      <div className="form-group">
        <label className="form-label">Join Type</label>
        <select
          value={joinType}
          onChange={(e) => onChange({ ...data, joinType: e.target.value })}
          className="form-select"
        >
          <option value="inner">INNER JOIN</option>
          <option value="left_outer">LEFT OUTER JOIN</option>
          <option value="right_outer">RIGHT OUTER JOIN</option>
          <option value="full_outer">FULL OUTER JOIN</option>
        </select>
      </div>

      <div className="form-group">
        <label className="form-label">ON Condition</label>
        <div className="space-y-2">
          <div className="flex items-center gap-1">
            <select
              value={leftStream}
              onChange={(e) => handleConditionChange(e.target.value, leftAttr, rightStream, rightAttr)}
              className="form-select w-24"
            >
              <option value="">stream</option>
              {allStreams.map((s) => (
                <option key={s.id} value={s.name}>{s.name}</option>
              ))}
            </select>
            <span className="text-gray-500">.</span>
            <AttributeSelect
              value={leftAttr}
              onChange={(val) => handleConditionChange(leftStream, val, rightStream, rightAttr)}
              attributes={leftStreamAttrs}
              placeholder="attribute"
              className="flex-1"
            />
          </div>
          <div className="text-center text-gray-500 text-xs">=</div>
          <div className="flex items-center gap-1">
            <select
              value={rightStream}
              onChange={(e) => handleConditionChange(leftStream, leftAttr, e.target.value, rightAttr)}
              className="form-select w-24"
            >
              <option value="">stream</option>
              {allStreams.map((s) => (
                <option key={s.id} value={s.name}>{s.name}</option>
              ))}
            </select>
            <span className="text-gray-500">.</span>
            <AttributeSelect
              value={rightAttr}
              onChange={(val) => handleConditionChange(leftStream, leftAttr, rightStream, val)}
              attributes={rightStreamAttrs}
              placeholder="attribute"
              className="flex-1"
            />
          </div>
        </div>
      </div>

      <div className="form-group">
        <label className="form-label">Trigger</label>
        <select
          value={trigger}
          onChange={(e) => onChange({ ...data, trigger: e.target.value })}
          className="form-select"
        >
          <option value="all">All (both streams)</option>
          <option value="left">Left stream only</option>
          <option value="right">Right stream only</option>
        </select>
      </div>

      <div className="form-group">
        <label className="form-label">WITHIN (optional)</label>
        <div className="flex items-center gap-2">
          <input
            type="number"
            value={within?.value || ''}
            onChange={(e) => {
              const val = parseInt(e.target.value);
              if (val > 0) {
                onChange({ ...data, within: { value: val, unit: within?.unit || 'SECONDS' } });
              } else {
                onChange({ ...data, within: undefined });
              }
            }}
            className="form-input flex-1"
            min={0}
            placeholder="duration"
          />
          <select
            value={within?.unit || 'SECONDS'}
            onChange={(e) => onChange({ ...data, within: within ? { ...within, unit: e.target.value } : undefined })}
            className="form-select w-24"
            disabled={!within?.value}
          >
            <option value="MILLISECONDS">ms</option>
            <option value="SECONDS">sec</option>
            <option value="MINUTES">min</option>
            <option value="HOURS">hour</option>
          </select>
        </div>
      </div>
    </div>
  );
}

// Pattern Property Editor
function PatternPropertyEditor({ data, onChange, allStreams }: { data: Record<string, unknown>; onChange: (changes: Record<string, unknown>) => void; allStreams: { id: string; name: string; attributes: UpstreamAttribute[] }[] }) {
  const mode = (data.mode as string) || 'pattern';
  const patternExpression = data.patternExpression as Record<string, unknown> || { type: 'stream' };
  const withinConstraint = data.withinConstraint as { type: string; value: number; unit?: string } | undefined;

  // Only simple patterns are supported for now, either one stream or
  // stream1 alias1 -> stream2 alias2. Filters on the streams cannot be set here.
  const leftStreamName = (patternExpression?.left as Record<string, unknown>)?.streamName as string ||
                         (patternExpression?.streamName as string) || '';
  const leftAlias = (patternExpression?.left as Record<string, unknown>)?.streamAlias as string ||
                    (patternExpression?.streamAlias as string) || '';
  const rightStreamName = (patternExpression?.right as Record<string, unknown>)?.streamName as string || '';
  const rightAlias = (patternExpression?.right as Record<string, unknown>)?.streamAlias as string || '';

  const handlePatternChange = (ls: string, la: string, rs: string, ra: string) => {
    if (!rs) {
      // Single stream pattern
      onChange({
        ...data,
        patternExpression: {
          type: 'stream',
          streamName: ls,
          streamAlias: la || undefined,
        },
      });
    } else {
      // Two streams joined with next (->)
      onChange({
        ...data,
        patternExpression: {
          type: 'next',
          operator: 'next',
          left: {
            type: 'stream',
            streamName: ls,
            streamAlias: la || undefined,
          },
          right: {
            type: 'stream',
            streamName: rs,
            streamAlias: ra || undefined,
          },
        },
      });
    }
  };

  return (
    <div className="space-y-4">
      <div className="form-group">
        <label className="form-label">Mode</label>
        <select
          value={mode}
          onChange={(e) => onChange({ ...data, mode: e.target.value })}
          className="form-select"
        >
          <option value="pattern">PATTERN (any order)</option>
          <option value="sequence">SEQUENCE (strict order)</option>
        </select>
      </div>

      <div className="form-group">
        <label className="form-label">Pattern Expression</label>
        <div className="space-y-3">
          {/* First stream */}
          <div className="p-2 bg-gray-800/50 rounded border border-gray-700">
            <div className="text-xs text-gray-500 mb-1">First event</div>
            <div className="flex items-center gap-2">
              <select
                value={leftStreamName}
                onChange={(e) => handlePatternChange(e.target.value, leftAlias, rightStreamName, rightAlias)}
                className="form-select flex-1"
              >
                <option value="">Select stream</option>
                {allStreams.map((s) => (
                  <option key={s.id} value={s.name}>{s.name}</option>
                ))}
              </select>
              <span className="text-gray-500 text-xs">AS</span>
              <input
                type="text"
                value={leftAlias}
                onChange={(e) => handlePatternChange(leftStreamName, e.target.value, rightStreamName, rightAlias)}
                className="form-input w-16"
                placeholder="alias"
              />
            </div>
          </div>

          <div className="text-center text-gray-400">→ followed by →</div>

          {/* Second stream */}
          <div className="p-2 bg-gray-800/50 rounded border border-gray-700">
            <div className="text-xs text-gray-500 mb-1">Second event (optional)</div>
            <div className="flex items-center gap-2">
              <select
                value={rightStreamName}
                onChange={(e) => handlePatternChange(leftStreamName, leftAlias, e.target.value, rightAlias)}
                className="form-select flex-1"
              >
                <option value="">Select stream</option>
                {allStreams.map((s) => (
                  <option key={s.id} value={s.name}>{s.name}</option>
                ))}
              </select>
              <span className="text-gray-500 text-xs">AS</span>
              <input
                type="text"
                value={rightAlias}
                onChange={(e) => handlePatternChange(leftStreamName, leftAlias, rightStreamName, e.target.value)}
                className="form-input w-16"
                placeholder="alias"
              />
            </div>
          </div>
        </div>
      </div>

      <div className="form-group">
        <label className="form-label">WITHIN Constraint (optional)</label>
        <div className="flex items-center gap-2">
          <input
            type="number"
            value={withinConstraint?.value || ''}
            onChange={(e) => {
              const val = parseInt(e.target.value);
              if (val > 0) {
                onChange({
                  ...data,
                  withinConstraint: {
                    type: withinConstraint?.type || 'time',
                    value: val,
                    unit: withinConstraint?.unit || 'SECONDS',
                  },
                });
              } else {
                onChange({ ...data, withinConstraint: undefined });
              }
            }}
            className="form-input flex-1"
            min={0}
            placeholder="value"
          />
          <select
            value={withinConstraint?.type || 'time'}
            onChange={(e) => onChange({
              ...data,
              withinConstraint: withinConstraint ? { ...withinConstraint, type: e.target.value } : undefined,
            })}
            className="form-select w-24"
            disabled={!withinConstraint?.value}
          >
            <option value="time">Time</option>
            <option value="event_count">Events</option>
          </select>
          {withinConstraint?.type === 'time' && (
            <select
              value={withinConstraint?.unit || 'SECONDS'}
              onChange={(e) => onChange({
                ...data,
                withinConstraint: withinConstraint ? { ...withinConstraint, unit: e.target.value } : undefined,
              })}
              className="form-select w-20"
              disabled={!withinConstraint?.value}
            >
              <option value="SECONDS">sec</option>
              <option value="MINUTES">min</option>
              <option value="HOURS">hour</option>
            </select>
          )}
        </div>
      </div>

      <div className="text-xs text-gray-500 italic">
        For complex patterns, use the SQL editor
      </div>
    </div>
  );
}

// Partition Property Editor
function PartitionPropertyEditor({ data, onChange, allStreams }: { data: Record<string, unknown>; onChange: (changes: Record<string, unknown>) => void; allStreams: { id: string; name: string; attributes: UpstreamAttribute[] }[] }) {
  const partitionBy = (data.partitionBy as { attribute: string; streamName: string }[]) || [];

  const handleAddPartition = () => {
    onChange({
      ...data,
      partitionBy: [
        ...partitionBy,
        { attribute: '', streamName: '' },
      ],
    });
  };

  const handleRemovePartition = (index: number) => {
    onChange({
      ...data,
      partitionBy: partitionBy.filter((_, i) => i !== index),
    });
  };

  const handlePartitionChange = (index: number, field: 'attribute' | 'streamName', value: string) => {
    const newPartitions = [...partitionBy];
    newPartitions[index] = {
      ...newPartitions[index],
      [field]: value,
    };
    onChange({ ...data, partitionBy: newPartitions });
  };

  const getStreamAttrs = (streamName: string): UpstreamAttribute[] => {
    return allStreams.find((s) => s.name === streamName)?.attributes || [];
  };

  return (
    <div className="space-y-4">
      <div className="form-group">
        <div className="flex items-center justify-between mb-2">
          <label className="form-label mb-0">Partition By</label>
          <button
            onClick={handleAddPartition}
            className="p-1 text-gray-400 hover:text-white transition-colors"
            title="Add partition attribute"
          >
            <Plus className="w-4 h-4" />
          </button>
        </div>

        <div className="space-y-2">
          {partitionBy.map((part, index) => (
            <div key={index} className="flex items-center gap-2">
              <select
                value={part.streamName}
                onChange={(e) => handlePartitionChange(index, 'streamName', e.target.value)}
                className="form-select w-24"
              >
                <option value="">stream</option>
                {allStreams.map((s) => (
                  <option key={s.id} value={s.name}>{s.name}</option>
                ))}
              </select>
              <span className="text-gray-500">.</span>
              <AttributeSelect
                value={part.attribute}
                onChange={(val) => handlePartitionChange(index, 'attribute', val)}
                attributes={getStreamAttrs(part.streamName)}
                placeholder="attribute"
                className="flex-1"
              />
              <button
                onClick={() => handleRemovePartition(index)}
                className="p-1 text-gray-500 hover:text-red-400 transition-colors"
              >
                <Trash2 className="w-3 h-3" />
              </button>
            </div>
          ))}
        </div>
      </div>

      <div className="text-xs text-gray-500">
        Partitioning creates independent processing instances for each unique combination of partition attribute values.
      </div>
    </div>
  );
}
