import { Play, Save, Settings, Undo, Redo, Layout, Maximize2, FolderOpen, Download } from 'lucide-react';
import { useReactFlow } from '@xyflow/react';
import { useApplicationStore } from '../../stores/applicationStore';
import { vscode } from '../../utils/vscode';

interface ToolbarProps {
  onOpenTemplates?: () => void;
}

export function Toolbar({ onOpenTemplates }: ToolbarProps) {
  const { name, isDirty, save, generatedSQL, viewMode, setViewMode, nodes, setNodes, undo, redo, past, future, pushHistory } = useApplicationStore();
  const { fitView } = useReactFlow();

  // Read from the subscribed past and future arrays so the buttons update on change
  const canUndo = past.length > 0;
  const canRedo = future.length > 0;

  const handleFitView = () => {
    fitView({ padding: 0.2, duration: 300 });
  };

  const handleAutoLayout = () => {
    if (nodes.length === 0) return;

    // Save the current state so the layout can be undone
    pushHistory();

    // Simple auto layout. Each node kind gets a column, left to right in flow order.
    // Sizes and gaps are in pixels.
    const nodeWidth = 200;
    const nodeHeight = 100;
    const horizontalGap = 80;
    const verticalGap = 60;
    const startX = 50;
    const startY = 50;

    // Every node that is not a source, sink, stream, table or trigger counts as processing
    const sourceNodes = nodes.filter(n => n.type === 'source');
    const triggerNodes = nodes.filter(n => n.type === 'trigger');
    const streamNodes = nodes.filter(n => n.type === 'stream');
    const processingNodes = nodes.filter(n => !['source', 'sink', 'stream', 'table', 'trigger'].includes(n.type || ''));
    const sinkNodes = nodes.filter(n => n.type === 'sink');
    const tableNodes = nodes.filter(n => n.type === 'table');

    let currentX = startX;
    let currentY = startY;
    const updatedNodes = [...nodes];

    // Column 1, sources
    sourceNodes.forEach((node, i) => {
      const idx = updatedNodes.findIndex(n => n.id === node.id);
      if (idx !== -1) {
        updatedNodes[idx] = {
          ...updatedNodes[idx],
          position: { x: currentX, y: currentY + i * (nodeHeight + verticalGap) },
        };
      }
    });

    // Column 1, triggers below the sources
    const sourceOffset = sourceNodes.length * (nodeHeight + verticalGap);
    triggerNodes.forEach((node, i) => {
      const idx = updatedNodes.findIndex(n => n.id === node.id);
      if (idx !== -1) {
        updatedNodes[idx] = {
          ...updatedNodes[idx],
          position: { x: currentX, y: currentY + sourceOffset + i * (nodeHeight + verticalGap) },
        };
      }
    });

    // Column 2, streams
    currentX += nodeWidth + horizontalGap;
    streamNodes.forEach((node, i) => {
      const idx = updatedNodes.findIndex(n => n.id === node.id);
      if (idx !== -1) {
        updatedNodes[idx] = {
          ...updatedNodes[idx],
          position: { x: currentX, y: currentY + i * (nodeHeight + verticalGap) },
        };
      }
    });

    // Column 3, processing nodes
    currentX += nodeWidth + horizontalGap;
    processingNodes.forEach((node, i) => {
      const idx = updatedNodes.findIndex(n => n.id === node.id);
      if (idx !== -1) {
        updatedNodes[idx] = {
          ...updatedNodes[idx],
          position: { x: currentX, y: currentY + i * (nodeHeight + verticalGap) },
        };
      }
    });

    // Column 4, sinks
    currentX += nodeWidth + horizontalGap;
    sinkNodes.forEach((node, i) => {
      const idx = updatedNodes.findIndex(n => n.id === node.id);
      if (idx !== -1) {
        updatedNodes[idx] = {
          ...updatedNodes[idx],
          position: { x: currentX, y: currentY + i * (nodeHeight + verticalGap) },
        };
      }
    });

    // Tables go in one row under the tallest column, starting at column 2
    currentX = startX + nodeWidth + horizontalGap;
    const maxY = Math.max(sourceNodes.length + triggerNodes.length, streamNodes.length, processingNodes.length, sinkNodes.length) * (nodeHeight + verticalGap) + startY;
    tableNodes.forEach((node, i) => {
      const idx = updatedNodes.findIndex(n => n.id === node.id);
      if (idx !== -1) {
        updatedNodes[idx] = {
          ...updatedNodes[idx],
          position: { x: currentX + i * (nodeWidth + horizontalGap), y: maxY + verticalGap },
        };
      }
    });

    setNodes(updatedNodes);

    // Wait 50 ms so React Flow has the new positions before fitting the view
    setTimeout(() => fitView({ padding: 0.2, duration: 300 }), 50);
  };

  const handleSave = () => {
    save();
  };

  const handleExport = () => {
    // The extension writes the SQL and builds the TOML config from the node data
    vscode.postMessage({
      type: 'export',
      sql: generatedSQL,
      nodes: nodes.map(n => ({
        type: n.type,
        data: n.data,
      })),
    });
  };

  const handleRun = () => {
    vscode.postMessage({ type: 'runSimulation', application: {} });
  };

  return (
    <div className="h-12 border-b border-vscode-border flex items-center px-4 gap-2 bg-gray-900/50">
      {/* App name */}
      <div className="flex items-center gap-2 mr-4">
        <span className="font-medium">{name}</span>
        {isDirty && <span className="text-xs text-gray-500">(unsaved)</span>}
      </div>

      <div className="h-6 w-px bg-vscode-border mx-2" />

      {/* File actions */}
      <ToolbarButton icon={Save} label="Save" onClick={handleSave} shortcut="Ctrl+S" />
      <ToolbarButton icon={FolderOpen} label="Templates" onClick={onOpenTemplates || (() => {})} />
      <ToolbarButton icon={Download} label="Export" onClick={handleExport} />

      <div className="h-6 w-px bg-vscode-border mx-2" />

      {/* Edit actions */}
      <ToolbarButton icon={Undo} label="Undo" onClick={undo} shortcut="Ctrl+Z" disabled={!canUndo} />
      <ToolbarButton icon={Redo} label="Redo" onClick={redo} shortcut="Ctrl+Y" disabled={!canRedo} />

      <div className="h-6 w-px bg-vscode-border mx-2" />

      {/* View actions */}
      <ToolbarButton icon={Layout} label="Auto Layout" onClick={handleAutoLayout} />
      <ToolbarButton icon={Maximize2} label="Fit View" onClick={handleFitView} />

      <div className="flex-1" />

      {/* View mode toggle */}
      <div className="flex items-center gap-1 bg-gray-800 rounded p-1">
        <ViewModeButton
          active={viewMode === 'visual'}
          onClick={() => setViewMode('visual')}
          label="Visual"
        />
        <ViewModeButton
          active={viewMode === 'split'}
          onClick={() => setViewMode('split')}
          label="Split"
        />
        <ViewModeButton
          active={viewMode === 'sql'}
          onClick={() => setViewMode('sql')}
          label="SQL"
        />
      </div>

      <div className="h-6 w-px bg-vscode-border mx-2" />

      {/* Run */}
      <button
        onClick={handleRun}
        className="flex items-center gap-2 px-3 py-1.5 bg-green-600 hover:bg-green-500 text-white rounded text-sm font-medium transition-colors"
      >
        <Play className="w-4 h-4" />
        Run
      </button>

      <ToolbarButton icon={Settings} label="Settings" onClick={() => {}} />
    </div>
  );
}

interface ToolbarButtonProps {
  icon: React.ComponentType<{ className?: string }>;
  label: string;
  onClick: () => void;
  shortcut?: string;
  disabled?: boolean;
}

function ToolbarButton({ icon: Icon, label, onClick, shortcut, disabled }: ToolbarButtonProps) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      className={`
        flex items-center gap-1.5 px-2 py-1.5 rounded text-sm
        transition-colors
        ${disabled
          ? 'text-gray-600 cursor-not-allowed'
          : 'text-gray-300 hover:bg-vscode-list-hover hover:text-white'
        }
      `}
      title={shortcut ? `${label} (${shortcut})` : label}
    >
      <Icon className="w-4 h-4" />
      <span className="hidden lg:inline">{label}</span>
    </button>
  );
}

interface ViewModeButtonProps {
  active: boolean;
  onClick: () => void;
  label: string;
}

function ViewModeButton({ active, onClick, label }: ViewModeButtonProps) {
  return (
    <button
      onClick={onClick}
      className={`
        px-3 py-1 rounded text-xs font-medium transition-colors
        ${active
          ? 'bg-vscode-button-bg text-white'
          : 'text-gray-400 hover:text-white'
        }
      `}
    >
      {label}
    </button>
  );
}
