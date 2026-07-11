// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  tutorialSidebar: [
    'intro',
    {
      type: 'category',
      label: 'Getting Started',
      collapsed: false,
      items: [
        'getting-started/installation',
        'getting-started/quick-start',
        'getting-started/running',
      ],
    },
    {
      type: 'category',
      label: 'Demos',
      collapsed: false,
      items: [
        'demo/crypto-trading',
      ],
    },
    {
      type: 'category',
      label: 'Configuration',
      collapsed: false,
      items: [
        'configuration/overview',
      ],
    },
    {
      type: 'category',
      label: 'SQL Reference',
      collapsed: false,
      items: [
        'sql-reference/queries',
        'sql-reference/windows',
        'sql-reference/aggregations',
        'sql-reference/joins',
        'sql-reference/tables',
        'sql-reference/patterns',
        'sql-reference/functions',
        'sql-reference/triggers',
      ],
    },
    {
      type: 'category',
      label: 'Connectors',
      collapsed: false,
      items: [
        'connectors/overview',
        'connectors/rabbitmq',
        'connectors/websocket',
        'connectors/mappers',
      ],
    },
    {
      type: 'category',
      label: 'Architecture',
      collapsed: true,
      items: [
        'architecture/overview',
        'architecture/event-pipeline',
        'architecture/state-management',
      ],
    },
    {
      type: 'category',
      label: 'Rust API',
      collapsed: true,
      items: [
        'rust-api/getting-started',
        'rust-api/configuration',
        'rust-api/testing',
        'rust-api/custom-connectors',
      ],
    },
  ],
};

export default sidebars;
