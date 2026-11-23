/**
 * EventFlux SQL parser for the studio.
 * Reads EventFlux SQL statements and turns them into visual model elements.
 * It is a small hand written parser and understands only a subset of the language.
 */

import type { VisualElement, ElementType, AttributeType, Expression, WindowType, AggregationType } from '../types';

// Token types
type TokenType =
  | 'KEYWORD'
  | 'IDENTIFIER'
  | 'NUMBER'
  | 'STRING'
  | 'OPERATOR'
  | 'PUNCTUATION'
  | 'WHITESPACE'
  | 'COMMENT'
  | 'EOF';

interface Token {
  type: TokenType;
  value: string;
  position: number;
}

// Parsed statement types
interface ParsedStream {
  type: 'stream';
  name: string;
  attributes: { name: string; type: string }[];
}

interface ParsedTable {
  type: 'table';
  name: string;
  attributes: { name: string; type: string }[];
  primaryKey?: string[];
  extension?: string;
  withConfig?: Record<string, string>;
}

interface ParsedTrigger {
  type: 'trigger';
  name: string;
  triggerType: 'start' | 'periodic' | 'cron';
  atEvery?: number;
  unit?: string;
  cronExpression?: string;
}

interface ParsedQuery {
  type: 'query';
  select: { expression: string; alias?: string }[];
  from: { source: string; alias?: string; window?: ParsedWindow }[];
  where?: string;
  groupBy?: string[];
  having?: string;
  insertInto: string;
  join?: { type: string; source: string; alias?: string; on: string; window?: ParsedWindow };
}

interface ParsedWindow {
  type: string;
  params: (string | number)[];
}

type ParsedStatement = ParsedStream | ParsedTable | ParsedTrigger | ParsedQuery;

// Keywords
const KEYWORDS = new Set([
  'CREATE', 'STREAM', 'TABLE', 'TRIGGER', 'DROP', 'ALTER',
  'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'UPDATE', 'DELETE',
  'GROUP', 'BY', 'HAVING', 'ORDER', 'LIMIT', 'OFFSET',
  'WINDOW', 'PARTITION', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER', 'ON',
  'AS', 'DISTINCT', 'ALL', 'AND', 'OR', 'NOT', 'IN', 'IS', 'NULL', 'BETWEEN', 'LIKE',
  'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'CAST',
  'PATTERN', 'SEQUENCE', 'EVERY', 'WITHIN', 'FOR', 'AT', 'START', 'CRON',
  'CURRENT', 'EXPIRED', 'EVENTS', 'OUTPUT', 'PRIMARY', 'KEY', 'WITH',
  'MILLISECONDS', 'SECONDS', 'MINUTES', 'HOURS', 'DAYS',
  'INT', 'INTEGER', 'LONG', 'BIGINT', 'FLOAT', 'DOUBLE', 'REAL',
  'STRING', 'VARCHAR', 'CHAR', 'TEXT', 'BOOL', 'BOOLEAN',
]);

/**
 * Splits SQL text into tokens. Always ends with an EOF token.
 * Characters it does not recognize are skipped.
 */
function tokenize(sql: string): Token[] {
  const tokens: Token[] = [];
  let pos = 0;

  while (pos < sql.length) {
    const char = sql[pos];

    // Whitespace
    if (/\s/.test(char)) {
      const start = pos;
      while (pos < sql.length && /\s/.test(sql[pos])) pos++;
      tokens.push({ type: 'WHITESPACE', value: sql.slice(start, pos), position: start });
      continue;
    }

    // Line comment, up to the newline
    if (sql.slice(pos, pos + 2) === '--') {
      const start = pos;
      while (pos < sql.length && sql[pos] !== '\n') pos++;
      tokens.push({ type: 'COMMENT', value: sql.slice(start, pos), position: start });
      continue;
    }

    // Block comment
    if (sql.slice(pos, pos + 2) === '/*') {
      const start = pos;
      pos += 2;
      while (pos < sql.length - 1 && sql.slice(pos, pos + 2) !== '*/') pos++;
      pos += 2;
      tokens.push({ type: 'COMMENT', value: sql.slice(start, pos), position: start });
      continue;
    }

    // Single quoted string. A backslash escapes the next character.
    if (char === "'") {
      const start = pos;
      pos++;
      while (pos < sql.length && sql[pos] !== "'") {
        if (sql[pos] === '\\') pos++;
        pos++;
      }
      pos++;
      tokens.push({ type: 'STRING', value: sql.slice(start, pos), position: start });
      continue;
    }

    // Double quoted text is an identifier, not a string. The quotes are kept.
    if (char === '"') {
      const start = pos;
      pos++;
      while (pos < sql.length && sql[pos] !== '"') pos++;
      pos++;
      tokens.push({ type: 'IDENTIFIER', value: sql.slice(start, pos), position: start });
      continue;
    }

    // Number, with an optional exponent
    if (/\d/.test(char) || (char === '.' && /\d/.test(sql[pos + 1] || ''))) {
      const start = pos;
      while (pos < sql.length && /[\d.]/.test(sql[pos])) pos++;
      if (sql[pos]?.toLowerCase() === 'e') {
        pos++;
        if (sql[pos] === '+' || sql[pos] === '-') pos++;
        while (pos < sql.length && /\d/.test(sql[pos])) pos++;
      }
      tokens.push({ type: 'NUMBER', value: sql.slice(start, pos), position: start });
      continue;
    }

    // Identifier or keyword
    if (/[a-zA-Z_]/.test(char)) {
      const start = pos;
      while (pos < sql.length && /[a-zA-Z0-9_]/.test(sql[pos])) pos++;
      const value = sql.slice(start, pos);
      const upper = value.toUpperCase();
      tokens.push({
        type: KEYWORDS.has(upper) ? 'KEYWORD' : 'IDENTIFIER',
        value: upper === value ? value : value, // keeps the original case for keywords and identifiers
        position: start,
      });
      continue;
    }

    // Operators
    if (/[=<>!+\-*/%&|^~]/.test(char)) {
      const start = pos;
      // Two character operators first, including -> for patterns
      const twoChar = sql.slice(pos, pos + 2);
      if (['<=', '>=', '!=', '<>', '||', '&&', '->'].includes(twoChar)) {
        pos += 2;
      } else {
        pos++;
      }
      tokens.push({ type: 'OPERATOR', value: sql.slice(start, pos), position: start });
      continue;
    }

    // Punctuation
    if (/[(),;.\[\]{}:]/.test(char)) {
      tokens.push({ type: 'PUNCTUATION', value: char, position: pos });
      pos++;
      continue;
    }

    // Unknown character, skip it
    pos++;
  }

  tokens.push({ type: 'EOF', value: '', position: pos });
  return tokens;
}

/**
 * Parses tokens into statements. Throws an Error with the expected and actual
 * token when a statement does not match.
 */
class Parser {
  private tokens: Token[];
  private pos: number = 0;

  constructor(sql: string) {
    this.tokens = tokenize(sql).filter(
      (t) => t.type !== 'WHITESPACE' && t.type !== 'COMMENT'
    );
  }

  private current(): Token {
    return this.tokens[this.pos] || { type: 'EOF', value: '', position: -1 };
  }

  private advance(): Token {
    const token = this.current();
    this.pos++;
    return token;
  }

  private expect(type: TokenType, value?: string): Token {
    const token = this.current();
    if (token.type !== type || (value !== undefined && token.value.toUpperCase() !== value.toUpperCase())) {
      throw new Error(`Expected ${type}${value ? ` '${value}'` : ''}, got ${token.type} '${token.value}'`);
    }
    return this.advance();
  }

  private match(type: TokenType, value?: string): boolean {
    const token = this.current();
    return token.type === type && (value === undefined || token.value.toUpperCase() === value.toUpperCase());
  }

  private matchKeyword(...keywords: string[]): boolean {
    return this.match('KEYWORD') && keywords.some((k) => this.current().value.toUpperCase() === k);
  }

  parse(): ParsedStatement[] {
    const statements: ParsedStatement[] = [];

    while (!this.match('EOF')) {
      if (this.matchKeyword('CREATE')) {
        statements.push(this.parseCreate());
      } else if (this.matchKeyword('SELECT', 'INSERT')) {
        statements.push(this.parseQuery());
      } else {
        // Skip anything that does not start a CREATE, SELECT or INSERT statement
        this.advance();
      }

      while (this.match('PUNCTUATION', ';')) {
        this.advance();
      }
    }

    return statements;
  }

  private parseCreate(): ParsedStatement {
    this.expect('KEYWORD', 'CREATE');

    if (this.matchKeyword('STREAM')) {
      return this.parseCreateStream();
    } else if (this.matchKeyword('TABLE')) {
      return this.parseCreateTable();
    } else if (this.matchKeyword('TRIGGER')) {
      return this.parseCreateTrigger();
    }

    throw new Error(`Unknown CREATE type: ${this.current().value}`);
  }

  private parseCreateStream(): ParsedStream {
    this.expect('KEYWORD', 'STREAM');
    const name = this.expect('IDENTIFIER').value;
    this.expect('PUNCTUATION', '(');
    const attributes = this.parseAttributeList();
    this.expect('PUNCTUATION', ')');

    return { type: 'stream', name, attributes };
  }

  private parseCreateTable(): ParsedTable {
    this.expect('KEYWORD', 'TABLE');
    const name = this.expect('IDENTIFIER').value;
    this.expect('PUNCTUATION', '(');
    const attributes = this.parseAttributeList();
    this.expect('PUNCTUATION', ')');

    const result: ParsedTable = { type: 'table', name, attributes };

    // Optional PRIMARY KEY (...)
    if (this.matchKeyword('PRIMARY')) {
      this.advance();
      this.expect('KEYWORD', 'KEY');
      this.expect('PUNCTUATION', '(');
      result.primaryKey = this.parseIdentifierList();
      this.expect('PUNCTUATION', ')');
    }

    // Optional WITH (...) clause. Keys and values must be single quoted strings.
    if (this.matchKeyword('WITH')) {
      this.advance();
      this.expect('PUNCTUATION', '(');
      result.withConfig = this.parseWithConfig();
      this.expect('PUNCTUATION', ')');
    }

    return result;
  }

  private parseCreateTrigger(): ParsedTrigger {
    this.expect('KEYWORD', 'TRIGGER');
    const name = this.expect('IDENTIFIER').value;
    this.expect('KEYWORD', 'AT');

    if (this.matchKeyword('START')) {
      this.advance();
      return { type: 'trigger', name, triggerType: 'start' };
    } else if (this.matchKeyword('EVERY')) {
      this.advance();
      const atEvery = parseInt(this.expect('NUMBER').value);
      const unit = this.expect('KEYWORD').value;
      return { type: 'trigger', name, triggerType: 'periodic', atEvery, unit };
    } else if (this.matchKeyword('CRON')) {
      this.advance();
      const cronExpression = this.expect('STRING').value.slice(1, -1); // strip the quotes
      return { type: 'trigger', name, triggerType: 'cron', cronExpression };
    }

    throw new Error(`Unknown trigger type: ${this.current().value}`);
  }

  private parseAttributeList(): { name: string; type: string }[] {
    const attributes: { name: string; type: string }[] = [];

    do {
      const name = this.expect('IDENTIFIER').value;
      const type = this.expect('KEYWORD').value;
      attributes.push({ name, type });
    } while (this.match('PUNCTUATION', ',') && this.advance());

    return attributes;
  }

  private parseIdentifierList(): string[] {
    const identifiers: string[] = [];

    do {
      identifiers.push(this.expect('IDENTIFIER').value);
    } while (this.match('PUNCTUATION', ',') && this.advance());

    return identifiers;
  }

  private parseWithConfig(): Record<string, string> {
    const config: Record<string, string> = {};

    do {
      const key = this.expect('STRING').value.slice(1, -1);
      this.expect('OPERATOR', '=');
      const value = this.expect('STRING').value.slice(1, -1);
      config[key] = value;
    } while (this.match('PUNCTUATION', ',') && this.advance());

    return config;
  }

  private parseQuery(): ParsedQuery {
    let insertInto = '';

    // INSERT INTO may come first or last. If both appear, the last one wins.
    if (this.matchKeyword('INSERT')) {
      this.advance();
      this.expect('KEYWORD', 'INTO');
      insertInto = this.expect('IDENTIFIER').value;
    }

    this.expect('KEYWORD', 'SELECT');
    // DISTINCT is accepted but ignored
    if (this.matchKeyword('DISTINCT')) this.advance();
    const select = this.parseSelectList();

    this.expect('KEYWORD', 'FROM');
    const from = this.parseFromClause();

    // At most one JOIN
    let join: ParsedQuery['join'];
    if (this.matchKeyword('JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL')) {
      join = this.parseJoinClause();
    }

    let where: string | undefined;
    if (this.matchKeyword('WHERE')) {
      this.advance();
      where = this.parseExpression();
    }

    let groupBy: string[] | undefined;
    if (this.matchKeyword('GROUP')) {
      this.advance();
      this.expect('KEYWORD', 'BY');
      groupBy = this.parseIdentifierList();
    }

    let having: string | undefined;
    if (this.matchKeyword('HAVING')) {
      this.advance();
      having = this.parseExpression();
    }

    if (this.matchKeyword('INSERT')) {
      this.advance();
      this.expect('KEYWORD', 'INTO');
      insertInto = this.expect('IDENTIFIER').value;
    }

    return {
      type: 'query',
      select,
      from,
      where,
      groupBy,
      having,
      insertInto,
      join,
    };
  }

  private parseSelectList(): { expression: string; alias?: string }[] {
    const items: { expression: string; alias?: string }[] = [];

    do {
      const expression = this.parseExpression();
      let alias: string | undefined;

      if (this.matchKeyword('AS')) {
        this.advance();
        alias = this.expect('IDENTIFIER').value;
      }

      items.push({ expression, alias });
    } while (this.match('PUNCTUATION', ',') && this.advance());

    return items;
  }

  private parseFromClause(): { source: string; alias?: string; window?: ParsedWindow }[] {
    const sources: { source: string; alias?: string; window?: ParsedWindow }[] = [];

    do {
      const source = this.expect('IDENTIFIER').value;
      let alias: string | undefined;
      let window: ParsedWindow | undefined;

      if (this.matchKeyword('AS')) {
        this.advance();
        alias = this.expect('IDENTIFIER').value;
      } else if (this.match('IDENTIFIER') && !this.matchKeyword('WINDOW', 'WHERE', 'GROUP', 'JOIN', 'INSERT', 'INNER', 'LEFT', 'RIGHT', 'FULL')) {
        alias = this.advance().value;
      }

      if (this.matchKeyword('WINDOW')) {
        window = this.parseWindowClause();
      }

      sources.push({ source, alias, window });
    } while (this.match('PUNCTUATION', ',') && this.advance());

    return sources;
  }

  private parseJoinClause(): ParsedQuery['join'] {
    let type = 'INNER';

    if (this.matchKeyword('INNER', 'LEFT', 'RIGHT', 'FULL')) {
      type = this.advance().value;
      if (this.matchKeyword('OUTER')) {
        this.advance();
      }
    }

    this.expect('KEYWORD', 'JOIN');
    const source = this.expect('IDENTIFIER').value;

    let alias: string | undefined;
    if (this.matchKeyword('AS')) {
      this.advance();
      alias = this.expect('IDENTIFIER').value;
    } else if (this.match('IDENTIFIER') && !this.matchKeyword('WINDOW', 'ON')) {
      alias = this.advance().value;
    }

    let window: ParsedWindow | undefined;
    if (this.matchKeyword('WINDOW')) {
      window = this.parseWindowClause();
    }

    this.expect('KEYWORD', 'ON');
    const on = this.parseExpression();

    return { type, source, alias, on, window };
  }

  private parseWindowClause(): ParsedWindow {
    this.expect('KEYWORD', 'WINDOW');
    this.expect('PUNCTUATION', '(');

    const windowType = this.expect('STRING').value.slice(1, -1);
    const params: (string | number)[] = [];

    while (this.match('PUNCTUATION', ',')) {
      this.advance();

      if (this.match('NUMBER')) {
        const num = this.advance().value;
        // A number followed by a time unit is kept as text, such as "5 SECONDS"
        if (this.matchKeyword('MILLISECONDS', 'SECONDS', 'MINUTES', 'HOURS', 'DAYS')) {
          params.push(`${num} ${this.advance().value}`);
        } else {
          params.push(parseFloat(num));
        }
      } else if (this.match('IDENTIFIER')) {
        params.push(this.advance().value);
      } else if (this.match('STRING')) {
        params.push(this.advance().value.slice(1, -1));
      }
    }

    this.expect('PUNCTUATION', ')');
    return { type: windowType, params };
  }

  private parseExpression(): string {
    // Does not build a tree. It joins token text up to the next clause keyword, a comma,
    // a semicolon or an unmatched ')' outside parentheses, and returns it as a string.
    const parts: string[] = [];
    let parenDepth = 0;

    while (!this.match('EOF')) {
      const token = this.current();

      if (parenDepth === 0) {
        if (token.type === 'PUNCTUATION' && [',', ';'].includes(token.value)) break;
        if (token.type === 'KEYWORD' && ['FROM', 'WHERE', 'GROUP', 'HAVING', 'ORDER', 'INSERT', 'JOIN', 'ON', 'AS', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'WINDOW'].includes(token.value.toUpperCase())) break;
      }

      if (token.value === '(') parenDepth++;
      if (token.value === ')') {
        if (parenDepth === 0) break;
        parenDepth--;
      }

      parts.push(token.value);
      this.advance();
    }

    return parts.join(' ').replace(/ ([,)])/g, '$1').replace(/([,(]) /g, '$1');
  }
}

/**
 * Parses SQL and turns each statement into visual elements laid out on the canvas.
 * On a parse error it returns no elements and the error message in errors.
 */
export function parseSQL(sql: string): { elements: VisualElement[]; errors: string[] } {
  const elements: VisualElement[] = [];
  const errors: string[] = [];

  try {
    const parser = new Parser(sql);
    const statements = parser.parse();

    let xPos = 100;
    let yPos = 100;
    const xSpacing = 250;
    const ySpacing = 150;
    let elementCount = 0;

    for (const stmt of statements) {
      const id = `${stmt.type}-${Date.now()}-${elementCount++}`;

      switch (stmt.type) {
        case 'stream':
          elements.push({
            id,
            type: 'stream' as ElementType,
            position: { x: xPos, y: yPos },
            properties: {
              streamName: stmt.name,
              attributes: stmt.attributes.map((a) => ({
                name: a.name,
                type: normalizeType(a.type) as AttributeType,
              })),
            },
          });
          yPos += ySpacing;
          break;

        case 'table':
          elements.push({
            id,
            type: 'table' as ElementType,
            position: { x: xPos, y: yPos },
            properties: {
              tableName: stmt.name,
              attributes: stmt.attributes.map((a) => ({
                name: a.name,
                type: normalizeType(a.type) as AttributeType,
              })),
              primaryKey: stmt.primaryKey,
              extension: stmt.extension,
              withConfig: stmt.withConfig,
            },
          });
          yPos += ySpacing;
          break;

        case 'trigger':
          elements.push({
            id,
            type: 'trigger' as ElementType,
            position: { x: xPos, y: yPos },
            properties: {
              triggerId: stmt.name,
              triggerType: stmt.triggerType,
              atEvery: stmt.atEvery ? convertToMilliseconds(stmt.atEvery, stmt.unit || 'MILLISECONDS') : undefined,
              cronExpression: stmt.cronExpression,
            },
          });
          yPos += ySpacing;
          break;

        case 'query':
          // A query becomes a row of elements, one column to the right of the definitions
          const queryElements = convertQueryToElements(stmt, xPos + xSpacing, yPos);
          elements.push(...queryElements);
          yPos += ySpacing * 2;
          break;
      }
    }
  } catch (e) {
    errors.push(e instanceof Error ? e.message : String(e));
  }

  return { elements, errors };
}

function normalizeType(type: string): string {
  const upper = type.toUpperCase();
  switch (upper) {
    case 'INTEGER':
    case 'INT':
      return 'INT';
    case 'BIGINT':
    case 'LONG':
      return 'LONG';
    case 'REAL':
    case 'FLOAT':
      return 'FLOAT';
    case 'DOUBLE':
      return 'DOUBLE';
    case 'VARCHAR':
    case 'CHAR':
    case 'TEXT':
    case 'STRING':
      return 'STRING';
    case 'BOOLEAN':
    case 'BOOL':
      return 'BOOL';
    default:
      return 'STRING';
  }
}

function normalizeWindowType(type: string): WindowType {
  const lower = type.toLowerCase();
  switch (lower) {
    case 'length':
      return 'length';
    case 'lengthbatch':
      return 'lengthBatch';
    case 'time':
      return 'time';
    case 'timebatch':
      return 'timeBatch';
    case 'tumbling':
      return 'tumbling';
    case 'sliding':
      return 'sliding';
    case 'session':
      return 'session';
    case 'externaltime':
      return 'externalTime';
    case 'externaltimebatch':
      return 'externalTimeBatch';
    case 'sort':
      return 'sort';
    default:
      return 'length';
  }
}

function convertToMilliseconds(value: number, unit: string): number {
  switch (unit.toUpperCase()) {
    case 'MILLISECONDS':
      return value;
    case 'SECONDS':
      return value * 1000;
    case 'MINUTES':
      return value * 60 * 1000;
    case 'HOURS':
      return value * 60 * 60 * 1000;
    case 'DAYS':
      return value * 24 * 60 * 60 * 1000;
    default:
      return value;
  }
}

function convertQueryToElements(query: ParsedQuery, startX: number, startY: number): VisualElement[] {
  const elements: VisualElement[] = [];
  let xPos = startX;
  let elementIndex = 0;

  // Only the window on the first FROM source is used. A JOIN is not turned into an element.
  if (query.from[0]?.window) {
    const window = query.from[0].window;
    elements.push({
      id: `window-${Date.now()}-${elementIndex++}`,
      type: 'window' as ElementType,
      position: { x: xPos, y: startY },
      properties: {
        windowType: normalizeWindowType(window.type),
        parameters: parseWindowParams(window),
      },
    });
    xPos += 200;
  }

  if (query.where) {
    const condition = parseExpressionToAst(query.where);
    if (condition) {
      elements.push({
        id: `filter-${Date.now()}-${elementIndex++}`,
        type: 'filter' as ElementType,
        position: { x: xPos, y: startY },
        properties: {
          condition,
        },
      });
      xPos += 200;
    }
  }

  // A select list with any aggregation call becomes an aggregation element holding only
  // the aggregations. Otherwise a select list other than * becomes a projection.
  const hasAggregations = query.select.some((s) =>
    /^(COUNT|SUM|AVG|MIN|MAX|FIRST|LAST|STDDEV|VARIANCE)\s*\(/i.test(s.expression)
  );

  if (hasAggregations) {
    // Without an alias, the alias is the function name in lower case
    const aggregations = query.select
      .filter((s) => /^(COUNT|SUM|AVG|MIN|MAX|FIRST|LAST|STDDEV|VARIANCE)\s*\(/i.test(s.expression))
      .map((s) => {
        const match = s.expression.match(/^(\w+)\s*\(\s*(.+?)\s*\)$/i);
        if (match) {
          return {
            type: match[1].toUpperCase() as AggregationType,
            expression: { type: 'variable' as const, variableName: match[2] } as Expression,
            alias: s.alias || match[1].toLowerCase(),
          };
        }
        return null;
      })
      .filter((a): a is NonNullable<typeof a> => a !== null);

    if (aggregations.length > 0) {
      elements.push({
        id: `aggregation-${Date.now()}-${elementIndex++}`,
        type: 'aggregation' as ElementType,
        position: { x: xPos, y: startY },
        properties: {
          aggregations,
        },
      });
      xPos += 200;
    }
  } else if (query.select.length > 0 && query.select[0].expression !== '*') {
    // Each select item is stored as a variable named by its raw expression text
    elements.push({
      id: `projection-${Date.now()}-${elementIndex++}`,
      type: 'projection' as ElementType,
      position: { x: xPos, y: startY },
      properties: {
        selectList: query.select.map((s) => ({
          expression: { type: 'variable' as const, variableName: s.expression },
          alias: s.alias,
        })),
      },
    });
    xPos += 200;
  }

  // HAVING is kept only when there is a GROUP BY
  if (query.groupBy && query.groupBy.length > 0) {
    const havingCondition = query.having ? parseExpressionToAst(query.having) : undefined;
    elements.push({
      id: `groupBy-${Date.now()}-${elementIndex++}`,
      type: 'groupBy' as ElementType,
      position: { x: xPos, y: startY },
      properties: {
        groupByAttributes: query.groupBy,
        havingCondition: havingCondition || undefined,
      },
    });
    xPos += 200;
  }

  // The INSERT INTO target becomes a new stream element that receives the results
  if (query.insertInto) {
    elements.push({
      id: `stream-${Date.now()}-${elementIndex++}`,
      type: 'stream' as ElementType,
      position: { x: xPos, y: startY },
      properties: {
        streamName: query.insertInto,
        attributes: [], // left empty. The parser does not work out the output schema.
      },
    });
  }

  return elements;
}

function parseWindowParams(window: ParsedWindow): Record<string, unknown> {
  const params: Record<string, unknown> = {};

  switch (window.type.toLowerCase()) {
    case 'length':
    case 'lengthbatch':
      params.count = typeof window.params[0] === 'number' ? window.params[0] : parseInt(String(window.params[0]));
      break;

    case 'time':
    case 'timebatch':
    case 'tumbling':
    case 'session':
      params.duration = parseDuration(window.params[0]);
      break;

    case 'sliding':
      params.duration = parseDuration(window.params[0]);
      if (window.params[1]) {
        params.slideInterval = parseDuration(window.params[1]);
      }
      break;

    case 'externaltime':
    case 'externaltimebatch':
      if (typeof window.params[0] === 'string' && !/\d/.test(window.params[0])) {
        params.timestampAttribute = window.params[0];
        params.duration = parseDuration(window.params[1]);
      } else {
        params.duration = parseDuration(window.params[0]);
      }
      break;

    case 'sort':
      params.count = typeof window.params[0] === 'number' ? window.params[0] : parseInt(String(window.params[0]));
      if (window.params[1]) {
        params.sortAttribute = window.params[1];
      }
      break;
  }

  return params;
}

function parseDuration(value: string | number | undefined): { value: number; unit: string } | undefined {
  if (value === undefined) return undefined;

  if (typeof value === 'number') {
    return { value, unit: 'MILLISECONDS' };
  }

  const match = String(value).match(/^(\d+)\s*(MILLISECONDS|SECONDS|MINUTES|HOURS|DAYS)$/i);
  if (match) {
    return { value: parseInt(match[1]), unit: match[2].toUpperCase() };
  }

  return { value: parseInt(String(value)), unit: 'MILLISECONDS' };
}

function parseExpressionToAst(expr: string): Expression | null {
  if (!expr || expr.trim() === '') return null;

  // A single comparison, attr op value. <> is stored as !=.
  const comparisonMatch = expr.match(/^(\w+(?:\.\w+)?)\s*(=|!=|<>|>|<|>=|<=)\s*(.+)$/);
  if (comparisonMatch) {
    const [, left, op, right] = comparisonMatch;
    const leftParts = left.split('.');
    const rightTrimmed = right.trim();
    const rightNum = parseFloat(rightTrimmed);
    const isString = rightTrimmed.startsWith("'") && rightTrimmed.endsWith("'");

    return {
      type: 'compare',
      operator: op === '<>' ? '!=' : op,
      left: {
        type: 'variable',
        variableName: leftParts.length > 1 ? leftParts[1] : leftParts[0],
        streamId: leftParts.length > 1 ? leftParts[0] : undefined,
      },
      right: {
        type: 'constant',
        constantType: isString ? 'string' : (!isNaN(rightNum) ? 'double' : 'string'),
        constantValue: isString ? rightTrimmed.slice(1, -1) : (!isNaN(rightNum) ? rightNum : rightTrimmed),
      },
    };
  }

  // Anything else is kept as a variable whose name is the whole expression text
  return {
    type: 'variable',
    variableName: expr,
  };
}

/**
 * Checks that the SQL parses. Stops at the first error, so errors holds at most one message.
 */
export function validateSQL(sql: string): { valid: boolean; errors: string[] } {
  const errors: string[] = [];

  try {
    const parser = new Parser(sql);
    parser.parse();
  } catch (e) {
    errors.push(e instanceof Error ? e.message : String(e));
  }

  return { valid: errors.length === 0, errors };
}
