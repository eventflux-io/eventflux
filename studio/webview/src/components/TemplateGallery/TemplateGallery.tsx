import { useState, useEffect } from 'react';
import { X, FileCode, Loader2 } from 'lucide-react';
import { vscode } from '../../utils/vscode';
import { useApplicationStore } from '../../stores/applicationStore';

interface Template {
  id: string;
  name: string;
  description: string;
}

interface TemplateGalleryProps {
  isOpen: boolean;
  onClose: () => void;
}

export function TemplateGallery({ isOpen, onClose }: TemplateGalleryProps) {
  const [templates, setTemplates] = useState<Template[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadingTemplate, setLoadingTemplate] = useState<string | null>(null);
  const { loadApplication } = useApplicationStore();

  useEffect(() => {
    if (!isOpen) return;

    // Ask the extension for the template list each time the gallery opens
    vscode.postMessage({ type: 'getTemplates' });
    setLoading(true);

    // Handles both the template list and a loaded template. A loaded template
    // replaces the current application and closes the gallery.
    const handleMessage = (event: MessageEvent) => {
      const message = event.data;
      if (message.type === 'templates') {
        setTemplates(message.templates || []);
        setLoading(false);
      } else if (message.type === 'templateLoaded') {
        if (message.template) {
          loadApplication(message.template);
          onClose();
        }
        setLoadingTemplate(null);
      }
    };

    window.addEventListener('message', handleMessage);
    return () => window.removeEventListener('message', handleMessage);
  }, [isOpen, loadApplication, onClose]);

  const handleLoadTemplate = (templateId: string) => {
    setLoadingTemplate(templateId);
    vscode.postMessage({ type: 'loadTemplate', templateId });
  };

  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-black/60"
        onClick={onClose}
      />

      {/* Modal */}
      <div className="relative bg-gray-900 rounded-lg shadow-xl border border-gray-700 w-full max-w-2xl max-h-[80vh] flex flex-col">
        {/* Header */}
        <div className="flex items-center justify-between px-4 py-3 border-b border-gray-700">
          <h2 className="text-lg font-medium text-white">Template Gallery</h2>
          <button
            onClick={onClose}
            className="p-1 text-gray-400 hover:text-white transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-4">
          {loading ? (
            <div className="flex items-center justify-center py-12">
              <Loader2 className="w-8 h-8 text-indigo-500 animate-spin" />
            </div>
          ) : templates.length === 0 ? (
            <div className="text-center py-12 text-gray-500">
              No templates available
            </div>
          ) : (
            <div className="grid gap-4">
              {templates.map((template) => (
                <button
                  key={template.id}
                  onClick={() => handleLoadTemplate(template.id)}
                  disabled={loadingTemplate !== null}
                  className="w-full text-left p-4 rounded-lg border border-gray-700 hover:border-indigo-500 hover:bg-gray-800/50 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  <div className="flex items-start gap-3">
                    <div className="p-2 bg-indigo-500/20 rounded-lg">
                      <FileCode className="w-6 h-6 text-indigo-400" />
                    </div>
                    <div className="flex-1">
                      <h3 className="font-medium text-white">{template.name}</h3>
                      <p className="text-sm text-gray-400 mt-1">
                        {template.description || 'No description'}
                      </p>
                    </div>
                    {loadingTemplate === template.id && (
                      <Loader2 className="w-5 h-5 text-indigo-500 animate-spin" />
                    )}
                  </div>
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="px-4 py-3 border-t border-gray-700 text-sm text-gray-500">
          Click a template to load it into the canvas
        </div>
      </div>
    </div>
  );
}
