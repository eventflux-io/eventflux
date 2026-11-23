// Wrapper around the VS Code webview API, used to talk to the extension

interface VSCodeAPI {
  postMessage(message: unknown): void;
  getState(): unknown;
  setState(state: unknown): void;
}

declare global {
  interface Window {
    acquireVsCodeApi?: () => VSCodeAPI;
  }
}

// Stand in API for running the webview outside VS Code. Messages go to the console
// and state is kept in localStorage under 'vscode-state'.
function createMockApi(): VSCodeAPI {
  console.warn('Running outside VS Code - using mock API');
  return {
    postMessage: (message) => {
      console.log('VS Code postMessage:', message);
    },
    getState: () => {
      const state = localStorage.getItem('vscode-state');
      return state ? JSON.parse(state) : undefined;
    },
    setState: (state) => {
      localStorage.setItem('vscode-state', JSON.stringify(state));
    },
  };
}

// acquireVsCodeApi exists only inside a VS Code webview. VS Code allows calling it
// once, so the result is kept here.
const vsCodeApi: VSCodeAPI = typeof window.acquireVsCodeApi === 'function'
  ? window.acquireVsCodeApi()
  : createMockApi();

export const vscode = {
  postMessage: (message: unknown) => {
    vsCodeApi.postMessage(message);
  },
  getState: <T>(): T | undefined => {
    return vsCodeApi.getState() as T | undefined;
  },
  setState: <T>(state: T) => {
    vsCodeApi.setState(state);
  },
};
