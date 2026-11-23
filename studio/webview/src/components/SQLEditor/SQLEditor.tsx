import Editor, { OnMount } from '@monaco-editor/react';
import { useApplicationStore } from '../../stores/applicationStore';
import { useCallback, useRef, useState } from 'react';
import type { editor, Position } from 'monaco-editor';
import { Upload, Check, AlertTriangle, RefreshCw } from 'lucide-react';

export function SQLEditor() {
  const { generatedSQL, viewMode, updateSQL, importSQL, getSQLValidation, regenerateSQL } = useApplicationStore();
  const editorRef = useRef<editor.IStandaloneCodeEditor | null>(null);
  const [isEdited, setIsEdited] = useState(false);
  const [validationErrors, setValidationErrors] = useState<string[]>([]);
  const [importStatus, setImportStatus] = useState<'idle' | 'success' | 'error'>('idle');

  const handleEditorDidMount: OnMount = useCallback((editor, monaco) => {
    editorRef.current = editor;

    // Monaco language for EventFlux SQL, which is SQL plus streaming keywords
    monaco.languages.register({ id: 'eventflux-sql' });

    // Syntax highlighting rules
    monaco.languages.setMonarchTokensProvider('eventflux-sql', {
      defaultToken: '',
      tokenPostfix: '.sql',
      ignoreCase: true,

      keywords: [
        // DDL
        'CREATE', 'STREAM', 'TABLE', 'TRIGGER', 'DROP', 'ALTER',
        // DML
        'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'UPDATE', 'DELETE',
        // Clauses
        'GROUP', 'BY', 'HAVING', 'ORDER', 'LIMIT', 'OFFSET',
        'WINDOW', 'PARTITION', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER', 'ON',
        'AS', 'DISTINCT', 'ALL', 'AND', 'OR', 'NOT', 'IN', 'IS', 'NULL', 'BETWEEN', 'LIKE',
        'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'CAST',
        // EventFlux extensions
        'PATTERN', 'SEQUENCE', 'EVERY', 'WITHIN', 'FOR', 'AT', 'START', 'CRON',
        'CURRENT', 'EXPIRED', 'EVENTS', 'OUTPUT', 'PRIMARY', 'KEY', 'WITH',
        // Time units
        'MILLISECONDS', 'SECONDS', 'MINUTES', 'HOURS', 'DAYS',
        // Window types
        'LENGTH', 'TIME', 'BATCH', 'TUMBLING', 'SLIDING', 'SESSION', 'SORT',
        // Aggregations
        'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'FIRST', 'LAST', 'STDDEV', 'VARIANCE',
      ],

      typeKeywords: [
        'INT', 'INTEGER', 'LONG', 'BIGINT', 'FLOAT', 'DOUBLE', 'REAL',
        'STRING', 'VARCHAR', 'CHAR', 'TEXT', 'BOOL', 'BOOLEAN',
        'TIMESTAMP', 'DATE', 'TIME', 'OBJECT',
      ],

      operators: [
        '=', '>', '<', '!', '~', '?', ':', '==', '<=', '>=', '!=',
        '&&', '||', '++', '--', '+', '-', '*', '/', '&', '|', '^', '%',
        '<<', '>>', '>>>', '+=', '-=', '*=', '/=', '&=', '|=', '^=',
        '%=', '<<=', '>>=', '>>>=',
      ],

      symbols: /[=><!~?:&|+\-*\/\^%]+/,

      escapes: /\\(?:[abfnrtv\\"']|x[0-9A-Fa-f]{1,4}|u[0-9A-Fa-f]{4}|U[0-9A-Fa-f]{8})/,

      tokenizer: {
        root: [
          // Comments
          [/--.*$/, 'comment'],
          [/\/\*/, 'comment', '@comment'],

          // Strings
          [/'([^'\\]|\\.)*$/, 'string.invalid'],
          [/'/, 'string', '@string_single'],
          [/"/, 'string', '@string_double'],

          // Numbers
          [/\d*\.\d+([eE][\-+]?\d+)?/, 'number.float'],
          [/\d+/, 'number'],

          // Words. Keywords and type names get their own color, the rest are identifiers.
          [/[a-zA-Z_]\w*/, {
            cases: {
              '@keywords': 'keyword',
              '@typeKeywords': 'type',
              '@default': 'identifier',
            },
          }],

          // Delimiters
          [/[{}()\[\]]/, '@brackets'],
          [/[;,.]/, 'delimiter'],

          // Operators
          [/@symbols/, {
            cases: {
              '@operators': 'operator',
              '@default': '',
            },
          }],
        ],

        comment: [
          [/[^\/*]+/, 'comment'],
          [/\*\//, 'comment', '@pop'],
          [/[\/*]/, 'comment'],
        ],

        string_single: [
          [/[^\\']+/, 'string'],
          [/@escapes/, 'string.escape'],
          [/\\./, 'string.escape.invalid'],
          [/'/, 'string', '@pop'],
        ],

        string_double: [
          [/[^\\"]+/, 'string'],
          [/@escapes/, 'string.escape'],
          [/\\./, 'string.escape.invalid'],
          [/"/, 'string', '@pop'],
        ],
      },
    });

    // Autocomplete. The list is static and does not depend on the cursor context.
    monaco.languages.registerCompletionItemProvider('eventflux-sql', {
      provideCompletionItems: (model: editor.ITextModel, position: Position) => {
        const word = model.getWordUntilPosition(position);
        const range = {
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: word.startColumn,
          endColumn: word.endColumn,
        };

        const suggestions = [
          // DDL statements
          { label: 'CREATE STREAM', kind: monaco.languages.CompletionItemKind.Snippet, insertText: 'CREATE STREAM ${1:StreamName} (\n  ${2:id} INT,\n  ${3:value} DOUBLE\n);', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet, detail: 'Create a new stream', range },
          { label: 'CREATE TABLE', kind: monaco.languages.CompletionItemKind.Snippet, insertText: 'CREATE TABLE ${1:TableName} (\n  ${2:key} STRING,\n  ${3:value} STRING\n) PRIMARY KEY (${2:key});', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet, detail: 'Create a new table', range },
          { label: 'CREATE TRIGGER', kind: monaco.languages.CompletionItemKind.Snippet, insertText: 'CREATE TRIGGER ${1:TriggerName} AT EVERY ${2:1000} MILLISECONDS;', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet, detail: 'Create a periodic trigger', range },

          // Query snippets
          { label: 'SELECT FROM', kind: monaco.languages.CompletionItemKind.Snippet, insertText: 'SELECT ${1:*}\nFROM ${2:StreamName}\nINSERT INTO ${3:OutputStream};', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet, detail: 'Basic query', range },
          { label: 'SELECT WITH WINDOW', kind: monaco.languages.CompletionItemKind.Snippet, insertText: 'SELECT ${1:*}\nFROM ${2:StreamName} WINDOW(\'${3:length}\', ${4:10})\nINSERT INTO ${5:OutputStream};', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet, detail: 'Query with window', range },
          { label: 'SELECT WITH FILTER', kind: monaco.languages.CompletionItemKind.Snippet, insertText: 'SELECT ${1:*}\nFROM ${2:StreamName}\nWHERE ${3:value} > ${4:100}\nINSERT INTO ${5:OutputStream};', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet, detail: 'Query with filter', range },
          { label: 'SELECT WITH GROUP BY', kind: monaco.languages.CompletionItemKind.Snippet, insertText: 'SELECT ${1:symbol}, COUNT(*) AS total\nFROM ${2:StreamName} WINDOW(\'${3:timeBatch}\', 5 SECONDS)\nGROUP BY ${1:symbol}\nINSERT INTO ${4:OutputStream};', insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet, detail: 'Query with aggregation', range },

          // Keywords
          ...['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'HAVING', 'ORDER BY', 'INSERT INTO', 'WINDOW', 'JOIN', 'ON', 'AND', 'OR', 'NOT', 'AS', 'DISTINCT'].map(kw => ({
            label: kw,
            kind: monaco.languages.CompletionItemKind.Keyword,
            insertText: kw,
            range,
          })),

          // Window functions
          ...['length', 'lengthBatch', 'time', 'timeBatch', 'tumbling', 'sliding', 'session', 'externalTime', 'sort'].map(fn => ({
            label: `WINDOW('${fn}')`,
            kind: monaco.languages.CompletionItemKind.Function,
            insertText: `WINDOW('${fn}', \${1:params})`,
            insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
            detail: `${fn} window`,
            range,
          })),

          // Aggregation functions
          ...['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'FIRST', 'LAST', 'STDDEV', 'VARIANCE'].map(fn => ({
            label: fn,
            kind: monaco.languages.CompletionItemKind.Function,
            insertText: `${fn}(\${1:*})`,
            insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
            detail: `${fn} aggregation`,
            range,
          })),

          // Data types
          ...['INT', 'LONG', 'DOUBLE', 'FLOAT', 'STRING', 'BOOL'].map(t => ({
            label: t,
            kind: monaco.languages.CompletionItemKind.TypeParameter,
            insertText: t,
            range,
          })),
        ];

        return { suggestions };
      },
    });

    editor.updateOptions({
      fontSize: 13,
      fontFamily: "'JetBrains Mono', 'Fira Code', Consolas, 'Courier New', monospace",
      minimap: { enabled: false },
      lineNumbers: 'on',
      renderLineHighlight: 'line',
      scrollBeyondLastLine: false,
      wordWrap: 'on',
      tabSize: 2,
      automaticLayout: true,
    });
  }, []);

  const handleEditorChange = useCallback((value: string | undefined) => {
    if (value !== undefined) {
      updateSQL(value);
      setIsEdited(true);
      setImportStatus('idle');

      // Validate on every edit so errors show while typing
      const { valid, errors } = getSQLValidation(value);
      setValidationErrors(valid ? [] : errors);
    }
  }, [updateSQL, getSQLValidation]);

  const handleImport = useCallback(() => {
    const { success, errors } = importSQL(generatedSQL);
    if (success) {
      setImportStatus('success');
      setIsEdited(false);
      setValidationErrors([]);
      // Clear the success badge after 2 seconds
      setTimeout(() => setImportStatus('idle'), 2000);
    } else {
      setImportStatus('error');
      setValidationErrors(errors);
    }
  }, [importSQL, generatedSQL]);

  const handleRegenerate = useCallback(() => {
    regenerateSQL();
    setIsEdited(false);
    setValidationErrors([]);
    setImportStatus('idle');
  }, [regenerateSQL]);

  // Hidden in visual mode
  if (viewMode === 'visual') {
    return null;
  }

  return (
    <div className={`flex flex-col ${viewMode === 'split' ? 'h-1/2 border-t border-vscode-border' : 'flex-1'}`}>
      <div className="px-3 py-2 bg-gray-900/50 border-b border-vscode-border flex items-center justify-between">
        <div className="flex items-center gap-3">
          <span className="text-xs text-gray-400 font-medium">EventFlux SQL</span>
          {isEdited && (
            <span className="text-xs text-yellow-500 flex items-center gap-1">
              <span className="w-2 h-2 bg-yellow-500 rounded-full"></span>
              Modified
            </span>
          )}
          {validationErrors.length > 0 && (
            <span className="text-xs text-red-400 flex items-center gap-1">
              <AlertTriangle size={12} />
              {validationErrors.length} error{validationErrors.length > 1 ? 's' : ''}
            </span>
          )}
          {importStatus === 'success' && (
            <span className="text-xs text-green-400 flex items-center gap-1">
              <Check size={12} />
              Imported successfully
            </span>
          )}
        </div>
        <div className="flex items-center gap-2">
          {isEdited && (
            <button
              onClick={handleRegenerate}
              className="text-xs px-2 py-1 bg-gray-700 hover:bg-gray-600 rounded flex items-center gap-1 text-gray-300"
              title="Discard changes and regenerate from visual model"
            >
              <RefreshCw size={12} />
              Reset
            </button>
          )}
          <button
            onClick={handleImport}
            disabled={validationErrors.length > 0}
            className={`text-xs px-2 py-1 rounded flex items-center gap-1 ${
              validationErrors.length > 0
                ? 'bg-gray-700 text-gray-500 cursor-not-allowed'
                : 'bg-indigo-600 hover:bg-indigo-500 text-white'
            }`}
            title="Import SQL to visual model"
          >
            <Upload size={12} />
            Import to Visual
          </button>
        </div>
      </div>

      {validationErrors.length > 0 && (
        <div className="px-3 py-2 bg-red-900/20 border-b border-red-900/50">
          <div className="text-xs text-red-400 space-y-1">
            {validationErrors.map((error, i) => (
              <div key={i} className="flex items-start gap-2">
                <AlertTriangle size={12} className="mt-0.5 flex-shrink-0" />
                <span>{error}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      <div className="flex-1">
        <Editor
          height="100%"
          defaultLanguage="eventflux-sql"
          value={generatedSQL}
          theme="vs-dark"
          onMount={handleEditorDidMount}
          onChange={handleEditorChange}
          options={{
            readOnly: false,
          }}
        />
      </div>
    </div>
  );
}
