import { useState } from 'react';
import type { Expression, AttributeType } from '../../types';

interface ExpressionBuilderProps {
  value: Expression | null;
  onChange: (expr: Expression) => void;
  availableAttributes?: { name: string; type: AttributeType; streamId?: string }[];
  placeholder?: string;
}

const COMPARISON_OPERATORS = ['=', '!=', '>', '<', '>=', '<='];

const FUNCTIONS = [
  // Aggregation
  { name: 'COUNT', category: 'Aggregation', params: 1 },
  { name: 'SUM', category: 'Aggregation', params: 1 },
  { name: 'AVG', category: 'Aggregation', params: 1 },
  { name: 'MIN', category: 'Aggregation', params: 1 },
  { name: 'MAX', category: 'Aggregation', params: 1 },
  { name: 'FIRST', category: 'Aggregation', params: 1 },
  { name: 'LAST', category: 'Aggregation', params: 1 },
  { name: 'STDDEV', category: 'Aggregation', params: 1 },
  { name: 'VARIANCE', category: 'Aggregation', params: 1 },
  // Math
  { name: 'ABS', category: 'Math', params: 1 },
  { name: 'CEIL', category: 'Math', params: 1 },
  { name: 'FLOOR', category: 'Math', params: 1 },
  { name: 'ROUND', category: 'Math', params: 1 },
  { name: 'SQRT', category: 'Math', params: 1 },
  { name: 'POWER', category: 'Math', params: 2 },
  // String
  { name: 'LENGTH', category: 'String', params: 1 },
  { name: 'UPPER', category: 'String', params: 1 },
  { name: 'LOWER', category: 'String', params: 1 },
  { name: 'TRIM', category: 'String', params: 1 },
  { name: 'CONCAT', category: 'String', params: -1 },
  { name: 'SUBSTRING', category: 'String', params: 3 },
  // Conditional
  { name: 'COALESCE', category: 'Conditional', params: -1 },
  { name: 'NULLIF', category: 'Conditional', params: 2 },
  { name: 'IF', category: 'Conditional', params: 3 },
];

export function ExpressionBuilder({ value, onChange, availableAttributes = [], placeholder }: ExpressionBuilderProps) {
  const [mode, setMode] = useState<'simple' | 'advanced'>('simple');

  if (mode === 'advanced') {
    return (
      <AdvancedExpressionBuilder
        value={value}
        onChange={onChange}
        availableAttributes={availableAttributes}
        onSwitchMode={() => setMode('simple')}
      />
    );
  }

  return (
    <SimpleExpressionBuilder
      value={value}
      onChange={onChange}
      availableAttributes={availableAttributes}
      placeholder={placeholder}
      onSwitchMode={() => setMode('advanced')}
    />
  );
}

// Builds one comparison, attribute operator value, from dropdowns and a text box.
// A value that parses as a number becomes a double constant, anything else a string.
function SimpleExpressionBuilder({
  value,
  onChange,
  availableAttributes,
  placeholder,
  onSwitchMode,
}: {
  value: Expression | null;
  onChange: (expr: Expression) => void;
  availableAttributes: { name: string; type: AttributeType; streamId?: string }[];
  placeholder?: string;
  onSwitchMode: () => void;
}) {
  // Read the current comparison. The operator defaults to '>'.
  const leftVar = (value?.left as Expression)?.variableName || '';
  const leftStream = (value?.left as Expression)?.streamId || '';
  const operator = value?.operator || '>';
  const rawRightValue = (value?.right as Expression)?.constantValue;
  const rightValue = rawRightValue !== null && rawRightValue !== undefined ? String(rawRightValue) : '';

  const handleChange = (left: string, leftStreamId: string, op: string, right: string) => {
    const rightNum = parseFloat(right);
    const isNumeric = !isNaN(rightNum);

    onChange({
      type: 'compare',
      operator: op,
      left: {
        type: 'variable',
        variableName: left,
        streamId: leftStreamId || undefined,
      },
      right: {
        type: 'constant',
        constantType: isNumeric ? 'double' : 'string',
        constantValue: isNumeric ? rightNum : right,
      },
    });
  };

  return (
    <div className="space-y-2">
      <div className="flex items-center gap-2">
        {/* Left side, the attribute */}
        <select
          value={leftStream ? `${leftStream}.${leftVar}` : leftVar}
          onChange={(e) => {
            const val = e.target.value;
            const parts = val.split('.');
            if (parts.length > 1) {
              handleChange(parts[1], parts[0], operator, String(rightValue));
            } else {
              handleChange(val, '', operator, String(rightValue));
            }
          }}
          className="form-select flex-1"
        >
          <option value="">{placeholder || 'Select attribute'}</option>
          {availableAttributes.map((attr, i) => (
            <option
              key={i}
              value={attr.streamId ? `${attr.streamId}.${attr.name}` : attr.name}
            >
              {attr.streamId ? `${attr.streamId}.${attr.name}` : attr.name}
            </option>
          ))}
          {availableAttributes.length === 0 && leftVar && (
            <option value={leftVar}>{leftVar}</option>
          )}
        </select>

        {/* Operator */}
        <select
          value={operator}
          onChange={(e) => handleChange(leftVar, leftStream, e.target.value, String(rightValue))}
          className="form-select w-16"
        >
          {COMPARISON_OPERATORS.map((op) => (
            <option key={op} value={op}>{op}</option>
          ))}
        </select>

        {/* Right side, the value */}
        <input
          type="text"
          value={rightValue}
          onChange={(e) => handleChange(leftVar, leftStream, operator, e.target.value)}
          className="form-input flex-1"
          placeholder="value"
        />
      </div>

      <button
        onClick={onSwitchMode}
        className="text-xs text-gray-500 hover:text-gray-300"
      >
        Switch to advanced mode
      </button>
    </div>
  );
}

// Free text expression input. Only a single comparison is understood, see
// parseSimpleExpression. Text it cannot parse leaves the last value unchanged.
function AdvancedExpressionBuilder({
  value,
  onChange,
  availableAttributes,
  onSwitchMode,
}: {
  value: Expression | null;
  onChange: (expr: Expression) => void;
  availableAttributes: { name: string; type: AttributeType; streamId?: string }[];
  onSwitchMode: () => void;
}) {
  const [rawInput, setRawInput] = useState(expressionToString(value));

  const handleRawChange = (input: string) => {
    setRawInput(input);
    // Regex parsing only. A real parser would be needed for AND, OR and functions.
    const expr = parseSimpleExpression(input);
    if (expr) {
      onChange(expr);
    }
  };

  return (
    <div className="space-y-2">
      <textarea
        value={rawInput}
        onChange={(e) => handleRawChange(e.target.value)}
        className="form-input w-full h-20 font-mono text-xs"
        placeholder="e.g., price > 100 AND volume < 1000"
      />

      <div className="flex items-center justify-between">
        <button
          onClick={onSwitchMode}
          className="text-xs text-gray-500 hover:text-gray-300"
        >
          Switch to simple mode
        </button>

        <div className="text-xs text-gray-500">
          Available: {availableAttributes.map(a => a.name).join(', ') || 'none'}
        </div>
      </div>
    </div>
  );
}

// Turns an expression back into text. AND and OR are wrapped in parentheses.
// Returns '' for null or an unknown expression type.
function expressionToString(expr: Expression | null): string {
  if (!expr) return '';

  switch (expr.type) {
    case 'constant':
      if (expr.constantType === 'string') return `'${expr.constantValue}'`;
      if (expr.constantType === 'null') return 'NULL';
      return String(expr.constantValue ?? '');

    case 'variable':
      return expr.streamId
        ? `${expr.streamId}.${expr.variableName}`
        : expr.variableName || '';

    case 'compare':
      return `${expressionToString(expr.left as Expression)} ${expr.operator} ${expressionToString(expr.right as Expression)}`;

    case 'and':
      return `(${expressionToString(expr.left as Expression)} AND ${expressionToString(expr.right as Expression)})`;

    case 'or':
      return `(${expressionToString(expr.left as Expression)} OR ${expressionToString(expr.right as Expression)})`;

    case 'function':
      const params = (expr.parameters || []).map(p => expressionToString(p as Expression)).join(', ');
      return `${expr.functionName}(${params})`;

    default:
      return '';
  }
}

// Parses one comparison like `price > 100` or `s.symbol = 'IBM'`.
// A quoted value becomes a string, a number becomes a double, anything else a string.
// Returns null for empty input or anything that is not a single comparison.
function parseSimpleExpression(input: string): Expression | null {
  if (!input.trim()) return null;

  const comparisonMatch = input.match(/^(\w+(?:\.\w+)?)\s*(=|!=|>|<|>=|<=)\s*(.+)$/);
  if (comparisonMatch) {
    const [, left, op, right] = comparisonMatch;
    const parts = left.split('.');
    const rightTrimmed = right.trim();
    const rightNum = parseFloat(rightTrimmed);
    const isString = rightTrimmed.startsWith("'") && rightTrimmed.endsWith("'");

    return {
      type: 'compare',
      operator: op,
      left: {
        type: 'variable',
        variableName: parts.length > 1 ? parts[1] : parts[0],
        streamId: parts.length > 1 ? parts[0] : undefined,
      },
      right: {
        type: 'constant',
        constantType: isString ? 'string' : (!isNaN(rightNum) ? 'double' : 'string'),
        constantValue: isString ? rightTrimmed.slice(1, -1) : (!isNaN(rightNum) ? rightNum : rightTrimmed),
      },
    };
  }

  return null;
}

// Dropdown of built in functions. Pass category to list only that group.
export function FunctionSelector({
  value,
  onChange,
  category,
}: {
  value: string;
  onChange: (fn: string) => void;
  category?: string;
}) {
  const filteredFunctions = category
    ? FUNCTIONS.filter(f => f.category === category)
    : FUNCTIONS;

  return (
    <select value={value} onChange={(e) => onChange(e.target.value)} className="form-select">
      <option value="">Select function</option>
      {filteredFunctions.map((fn) => (
        <option key={fn.name} value={fn.name}>{fn.name}</option>
      ))}
    </select>
  );
}

// Dropdown of attributes with their types. Switches to a text box when the user picks
// custom or when value is not in the list.
export function AttributeSelector({
  value,
  onChange,
  attributes,
  placeholder,
  allowCustom = true,
}: {
  value: string;
  onChange: (attr: string) => void;
  attributes: { name: string; type: AttributeType; streamId?: string }[];
  placeholder?: string;
  allowCustom?: boolean;
}) {
  const [isCustom, setIsCustom] = useState(false);

  if (isCustom || (value && !attributes.find(a => a.name === value))) {
    return (
      <div className="flex items-center gap-1">
        <input
          type="text"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          className="form-input flex-1"
          placeholder={placeholder || 'attribute'}
        />
        {attributes.length > 0 && (
          <button
            onClick={() => setIsCustom(false)}
            className="text-xs text-gray-500 hover:text-gray-300"
          >
            list
          </button>
        )}
      </div>
    );
  }

  return (
    <div className="flex items-center gap-1">
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="form-select flex-1"
      >
        <option value="">{placeholder || 'Select attribute'}</option>
        {attributes.map((attr, i) => (
          <option key={i} value={attr.name}>
            {attr.name} ({attr.type})
          </option>
        ))}
      </select>
      {allowCustom && (
        <button
          onClick={() => setIsCustom(true)}
          className="text-xs text-gray-500 hover:text-gray-300"
        >
          custom
        </button>
      )}
    </div>
  );
}
