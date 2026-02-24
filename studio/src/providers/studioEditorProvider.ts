import * as vscode from 'vscode';
import * as path from 'path';

export class StudioEditorProvider implements vscode.CustomTextEditorProvider {
  public static readonly viewType = 'eventflux.studioEditor';

  private static readonly webviewOptions: vscode.WebviewPanelOptions & vscode.WebviewOptions = {
    enableScripts: true,
    retainContextWhenHidden: true,
  };

  constructor(private readonly context: vscode.ExtensionContext) {}

  public static register(context: vscode.ExtensionContext): vscode.Disposable {
    const provider = new StudioEditorProvider(context);
    return vscode.window.registerCustomEditorProvider(
      StudioEditorProvider.viewType,
      provider,
      {
        webviewOptions: {
          retainContextWhenHidden: true,
        },
        supportsMultipleEditorsPerDocument: false,
      }
    );
  }

  public async resolveCustomTextEditor(
    document: vscode.TextDocument,
    webviewPanel: vscode.WebviewPanel,
    _token: vscode.CancellationToken
  ): Promise<void> {
    // Set webview options
    webviewPanel.webview.options = {
      enableScripts: true,
      localResourceRoots: [
        vscode.Uri.joinPath(this.context.extensionUri, 'webview', 'dist'),
        vscode.Uri.joinPath(this.context.extensionUri, 'templates'),
      ],
    };

    // Set the webview HTML content
    webviewPanel.webview.html = this.getHtmlForWebview(webviewPanel.webview);

    // Handle messages from the webview
    const messageDisposable = webviewPanel.webview.onDidReceiveMessage(
      (message) => this.handleMessage(document, webviewPanel, message)
    );

    // Handle document changes
    const changeDocumentDisposable = vscode.workspace.onDidChangeTextDocument((e) => {
      if (e.document.uri.toString() === document.uri.toString()) {
        this.updateWebview(webviewPanel.webview, document);
      }
    });

    // Clean up on dispose
    webviewPanel.onDidDispose(() => {
      messageDisposable.dispose();
      changeDocumentDisposable.dispose();
    });

    // Initial load
    this.updateWebview(webviewPanel.webview, document);
  }

  private getHtmlForWebview(webview: vscode.Webview): string {
    // Get URIs for webview resources
    const scriptUri = webview.asWebviewUri(
      vscode.Uri.joinPath(this.context.extensionUri, 'webview', 'dist', 'index.js')
    );
    const styleUri = webview.asWebviewUri(
      vscode.Uri.joinPath(this.context.extensionUri, 'webview', 'dist', 'index.css')
    );

    // Use a nonce to only allow specific scripts to run
    const nonce = getNonce();

    return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta http-equiv="Content-Security-Policy" content="default-src 'none'; style-src ${webview.cspSource} 'unsafe-inline' https://cdn.jsdelivr.net; script-src 'nonce-${nonce}' 'unsafe-eval' https://cdn.jsdelivr.net; img-src ${webview.cspSource} https: data:; font-src ${webview.cspSource} https://cdn.jsdelivr.net; connect-src ${webview.cspSource} https://cdn.jsdelivr.net; worker-src blob:;">
  <link href="${styleUri}" rel="stylesheet">
  <title>EventFlux Studio</title>
</head>
<body>
  <div id="root"></div>
  <script nonce="${nonce}" src="${scriptUri}"></script>
</body>
</html>`;
  }

  private updateWebview(webview: vscode.Webview, document: vscode.TextDocument): void {
    try {
      const content = document.getText();
      const data = content ? JSON.parse(content) : null;
      webview.postMessage({
        type: 'load',
        data,
        fileName: path.basename(document.fileName),
      });
    } catch (error) {
      console.error('Failed to parse studio file:', error);
      webview.postMessage({
        type: 'error',
        message: 'Failed to parse studio file. Please check the JSON format.',
      });
    }
  }

  private async handleMessage(
    document: vscode.TextDocument,
    panel: vscode.WebviewPanel,
    message: WebviewMessage
  ): Promise<void> {
    switch (message.type) {
      case 'ready':
        // Webview is ready, send initial data
        this.updateWebview(panel.webview, document);
        break;

      case 'update':
        // Update document with new content
        await this.updateDocument(document, message.content);
        break;

      case 'generateSQL':
        // Generate SQL from visual model
        const sql = await this.generateSQL(message.application);
        panel.webview.postMessage({ type: 'sqlGenerated', sql });
        break;

      case 'showSQL':
        // Show generated SQL in new editor
        if (message.sql) {
          await this.showGeneratedSQL(message.sql);
        }
        break;

      case 'saveSQL':
        // Save SQL to file (legacy, kept for backwards compatibility)
        if (message.sql) {
          await this.saveSQL(document, message.sql);
        }
        break;

      case 'export':
        // Export SQL and TOML config files
        await this.exportApplication(document, message.sql, message.nodes);
        break;

      case 'getTemplates':
        // Return available templates
        const templates = await this.getTemplates();
        panel.webview.postMessage({ type: 'templates', templates });
        break;

      case 'loadTemplate':
        // Load a specific template
        if (message.templateId) {
          const template = await this.loadTemplate(message.templateId);
          panel.webview.postMessage({ type: 'templateLoaded', template });
        }
        break;

      case 'runSimulation':
        // Run simulation with test data
        const result = await this.runSimulation(message.application, message.testData);
        panel.webview.postMessage({ type: 'simulationResult', result });
        break;

      case 'getConfig':
        // Return VS Code configuration
        const config = vscode.workspace.getConfiguration('eventflux');
        panel.webview.postMessage({
          type: 'config',
          config: {
            gridSize: config.get('studio.gridSize', 20),
            snapToGrid: config.get('studio.snapToGrid', true),
            autoSave: config.get('studio.autoSave', true),
            engineHost: config.get('host', 'localhost'),
            enginePort: config.get('port', 9090),
          },
        });
        break;
    }
  }

  private async updateDocument(document: vscode.TextDocument, content: unknown): Promise<void> {
    const edit = new vscode.WorkspaceEdit();
    edit.replace(
      document.uri,
      new vscode.Range(0, 0, document.lineCount, 0),
      JSON.stringify(content, null, 2)
    );
    const success = await vscode.workspace.applyEdit(edit);
    if (success) {
      // Save the document to disk
      await document.save();
    }
  }

  private async generateSQL(application: unknown): Promise<string> {
    // TODO: Implement proper SQL generation
    // For now, return a placeholder
    return '-- Generated by EventFlux Studio\n-- Implementation coming soon';
  }

  private async showGeneratedSQL(sql: string): Promise<void> {
    const doc = await vscode.workspace.openTextDocument({
      content: sql,
      language: 'sql',
    });
    await vscode.window.showTextDocument(doc, vscode.ViewColumn.Beside);
  }

  private async saveSQL(document: vscode.TextDocument, sql: string): Promise<void> {
    const basePath = document.uri.fsPath.replace('.eventflux.studio', '').replace('.efstudio', '');
    const sqlUri = vscode.Uri.file(`${basePath}.eventflux`);
    await vscode.workspace.fs.writeFile(sqlUri, Buffer.from(sql));
    vscode.window.showInformationMessage(`SQL saved to ${path.basename(sqlUri.fsPath)}`);
  }

  private async exportApplication(
    document: vscode.TextDocument,
    sql: string | undefined,
    nodes: NodeData[] | undefined
  ): Promise<void> {
    try {
      const basePath = document.uri.fsPath.replace('.eventflux.studio', '').replace('.efstudio', '');
      const exportedFiles: string[] = [];

      // Export SQL file
      if (sql) {
        const sqlUri = vscode.Uri.file(`${basePath}.eventflux`);
        await vscode.workspace.fs.writeFile(sqlUri, Buffer.from(sql));
        exportedFiles.push(path.basename(sqlUri.fsPath));
      }

      // Generate and export TOML config if there are sources/sinks with config
      if (nodes && nodes.length > 0) {
        const toml = this.generateTOML(nodes);
        if (toml) {
          const tomlUri = vscode.Uri.file(`${basePath}.toml`);
          await vscode.workspace.fs.writeFile(tomlUri, Buffer.from(toml));
          exportedFiles.push(path.basename(tomlUri.fsPath));
        }
      }

      if (exportedFiles.length > 0) {
        vscode.window.showInformationMessage(`Exported: ${exportedFiles.join(', ')}`);
      } else {
        vscode.window.showWarningMessage('Nothing to export');
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      vscode.window.showErrorMessage(`Export failed: ${message}`);
    }
  }

  private generateTOML(nodes: NodeData[]): string | null {
    const lines: string[] = [
      '# EventFlux Configuration',
      '# Generated by EventFlux Studio',
      '',
    ];

    // Collect source and sink configurations
    const sources = nodes.filter(n => n.type === 'source');
    const sinks = nodes.filter(n => n.type === 'sink');
    const tables = nodes.filter(n => n.type === 'table');

    // Check if any node has config to export
    const hasConfig = [...sources, ...sinks, ...tables].some(
      n => n.data?.config && Object.keys(n.data.config).length > 0
    );

    if (!hasConfig) {
      return null;
    }

    // Generate [application] section with common defaults
    lines.push('[application]');
    lines.push('buffer_size = "1024"');
    lines.push('');

    // Generate [streams.Name] sections for sources
    for (const source of sources) {
      const data = source.data as SourceData;
      if (!data?.sourceName || !data?.config || Object.keys(data.config).length === 0) {
        continue;
      }

      lines.push(`[streams.${data.sourceName}]`);
      const sourceType = data.sourceType || 'websocket';

      // Group config by connector type prefix
      for (const [key, value] of Object.entries(data.config)) {
        // Convert common config keys to TOML format
        const tomlKey = this.convertConfigKey(key, sourceType);
        lines.push(`${tomlKey} = "${this.escapeTomlValue(String(value))}"`);
      }
      lines.push('');
    }

    // Generate sink configurations (as output streams)
    for (const sink of sinks) {
      const data = sink.data as SinkData;
      if (!data?.sinkName || !data?.config || Object.keys(data.config).length === 0) {
        continue;
      }

      lines.push(`[streams.${data.sinkName}]`);
      const sinkType = data.sinkType || 'log';

      for (const [key, value] of Object.entries(data.config)) {
        const tomlKey = this.convertConfigKey(key, sinkType);
        lines.push(`${tomlKey} = "${this.escapeTomlValue(String(value))}"`);
      }
      lines.push('');
    }

    // Generate [tables.Name] sections
    for (const table of tables) {
      const data = table.data as TableData;
      if (!data?.tableName || !data?.config || Object.keys(data.config).length === 0) {
        continue;
      }

      lines.push(`[tables.${data.tableName}]`);
      for (const [key, value] of Object.entries(data.config)) {
        lines.push(`${key} = "${this.escapeTomlValue(String(value))}"`);
      }
      lines.push('');
    }

    return lines.join('\n');
  }

  private escapeTomlValue(value: string): string {
    // Escape special characters for TOML string values
    return value
      .replace(/\\/g, '\\\\')
      .replace(/"/g, '\\"')
      .replace(/\n/g, '\\n')
      .replace(/\r/g, '\\r')
      .replace(/\t/g, '\\t');
  }

  private convertConfigKey(key: string, connectorType: string): string {
    // Convert common config keys like "bootstrap.servers" to "kafka.brokers"
    const keyMappings: Record<string, Record<string, string>> = {
      kafka: {
        'bootstrap.servers': 'kafka.brokers',
        'topic': 'kafka.topic',
        'group.id': 'kafka.group_id',
        'auto.offset.reset': 'kafka.auto_offset_reset',
      },
      http: {
        'url': 'http.url',
        'method': 'http.method',
        'headers': 'http.headers',
      },
      mqtt: {
        'broker': 'mqtt.broker',
        'topic': 'mqtt.topic',
        'qos': 'mqtt.qos',
      },
      redis: {
        'host': 'redis.host',
        'port': 'redis.port',
        'key_prefix': 'redis.key_prefix',
      },
    };

    const mapping = keyMappings[connectorType];
    if (mapping && mapping[key]) {
      return mapping[key];
    }

    // If no mapping, prefix with connector type if not already prefixed
    if (!key.includes('.')) {
      return `${connectorType}.${key}`;
    }
    return key;
  }

  private async getTemplates(): Promise<TemplateInfo[]> {
    const templatesDir = vscode.Uri.joinPath(this.context.extensionUri, 'templates');
    try {
      const files = await vscode.workspace.fs.readDirectory(templatesDir);
      const templates: TemplateInfo[] = [];

      for (const [name, type] of files) {
        if (type === vscode.FileType.File && name.endsWith('.json')) {
          const uri = vscode.Uri.joinPath(templatesDir, name);
          const content = await vscode.workspace.fs.readFile(uri);
          const data = JSON.parse(content.toString());
          templates.push({
            id: name.replace('.json', ''),
            name: data.name || name,
            description: data.description || '',
          });
        }
      }

      return templates;
    } catch {
      return [];
    }
  }

  private async loadTemplate(templateId: string): Promise<unknown> {
    const templateUri = vscode.Uri.joinPath(
      this.context.extensionUri,
      'templates',
      `${templateId}.json`
    );
    const content = await vscode.workspace.fs.readFile(templateUri);
    return JSON.parse(content.toString());
  }

  private async runSimulation(application: unknown, testData: unknown): Promise<unknown> {
    // TODO: Implement simulation engine
    return {
      success: true,
      message: 'Simulation not yet implemented',
      steps: [],
    };
  }
}

interface WebviewMessage {
  type: string;
  content?: unknown;
  application?: unknown;
  sql?: string;
  templateId?: string;
  testData?: unknown;
  nodes?: NodeData[];
}

interface NodeData {
  type: string;
  data: Record<string, unknown>;
}

interface SourceData {
  sourceName?: string;
  sourceType?: string;
  config?: Record<string, string>;
}

interface SinkData {
  sinkName?: string;
  sinkType?: string;
  config?: Record<string, string>;
}

interface TableData {
  tableName?: string;
  config?: Record<string, string>;
}

interface TemplateInfo {
  id: string;
  name: string;
  description: string;
}

function getNonce(): string {
  let text = '';
  const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  for (let i = 0; i < 32; i++) {
    text += possible.charAt(Math.floor(Math.random() * possible.length));
  }
  return text;
}
