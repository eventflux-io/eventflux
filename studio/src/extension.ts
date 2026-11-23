import * as vscode from 'vscode';
import { StudioEditorProvider } from './providers/studioEditorProvider';

export function activate(context: vscode.ExtensionContext) {
  console.log('EventFlux Studio extension is now active');

  // Register the custom editor provider for .eventflux.studio files
  context.subscriptions.push(StudioEditorProvider.register(context));

  // Command that opens the active .eventflux file in the studio
  context.subscriptions.push(
    vscode.commands.registerCommand('eventflux.openStudio', async () => {
      const editor = vscode.window.activeTextEditor;
      if (editor && editor.document.fileName.endsWith('.eventflux')) {
        const sqlContent = editor.document.getText();
        await createStudioFromSQL(sqlContent, editor.document.uri);
      } else {
        vscode.window.showInformationMessage(
          'Please open an EventFlux (.eventflux) file first'
        );
      }
    })
  );

  // Command that creates an empty studio project at a path the user picks
  context.subscriptions.push(
    vscode.commands.registerCommand('eventflux.newStudioProject', async () => {
      const uri = await vscode.window.showSaveDialog({
        filters: {
          'EventFlux Studio': ['eventflux.studio', 'efstudio'],
        },
        saveLabel: 'Create Studio Project',
      });

      if (uri) {
        const emptyProject = createEmptyProject();
        await vscode.workspace.fs.writeFile(
          uri,
          Buffer.from(JSON.stringify(emptyProject, null, 2))
        );
        await vscode.commands.executeCommand('vscode.openWith', uri, 'eventflux.studioEditor');
      }
    })
  );
}

export function deactivate() {
  console.log('EventFlux Studio extension is now deactivated');
}

async function createStudioFromSQL(sqlContent: string, sourceUri: vscode.Uri): Promise<void> {
  // Write <name>.eventflux.studio next to the source file and store the raw SQL
  // in importedSQL. The SQL is not parsed here. An existing file is overwritten.
  const baseName = sourceUri.path.replace('.eventflux', '');
  const studioUri = vscode.Uri.file(`${baseName}.eventflux.studio`);

  const project = createEmptyProject();
  project.importedSQL = sqlContent;
  project.metadata.importedFrom = sourceUri.fsPath;

  await vscode.workspace.fs.writeFile(
    studioUri,
    Buffer.from(JSON.stringify(project, null, 2))
  );

  await vscode.commands.executeCommand('vscode.openWith', studioUri, 'eventflux.studioEditor');
}

interface StudioProject {
  $schema: string;
  version: string;
  name: string;
  application: {
    elements: unknown[];
    connections: unknown[];
  };
  layout: {
    zoom: number;
    pan: { x: number; y: number };
    gridSize: number;
    snapToGrid: boolean;
  };
  metadata: {
    created: string;
    modified: string;
    importedFrom?: string;
  };
  importedSQL?: string;
}

function createEmptyProject(): StudioProject {
  const now = new Date().toISOString();
  return {
    $schema: 'https://eventflux.io/schemas/studio/v1.json',
    version: '1.0',
    name: 'Untitled Project',
    application: {
      elements: [],
      connections: [],
    },
    layout: {
      zoom: 1.0,
      pan: { x: 0, y: 0 },
      gridSize: 20,
      snapToGrid: true,
    },
    metadata: {
      created: now,
      modified: now,
    },
  };
}
